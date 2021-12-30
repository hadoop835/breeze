mod bulk;
mod command;
mod token;

use bulk::*;
use std::str::from_utf8;
use token::*;

use crate::{
    error::ProtocolType, redis::command::PADDING_RSP_TABLE, Command, Commander, Error, Flag,
    HashedCommand, Protocol, RequestProcessor, Result, Stream,
};
use ds::{MemGuard, RingSlice};
use sharding::hash::Hash;

// redis 协议最多支持10w个token
const MAX_BULK_COUNT: usize = 100000;
// 最大消息支持1M
const MAX_MSG_LEN: usize = 1000000;

#[derive(Clone, Default)]
pub struct Redis;

impl Redis {
    // 一条redis消息，包含多个token，每个token有2部分，meta部分记录长度信息，数据部分是有效信息。
    // eg：let s = b"*5\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n";
    // 上面的redis协议，有5个token，分别是mset k1 v1 k2 v2，每个token前面的$len即为meta
    // 快速轮询方案：
    //    1 确认bulk数量；
    //    2 获得第一个bulk，即cmdname，得到协议的property；
    //    3 对于单key，直接根据key位置确定key的bulk，并根据bulk数量定位到协议末尾，然后处理协议；
    //    4 对于multi-key，根据\r\n找到所有bulk，并找到所有keys的bulk，然后构建协议轮询处理协议；
    // TODO: 返回的error，如果是ProtocolIncomplete，说明是协议没有读取完毕，后续需要继续读
    #[inline(always)]
    fn parse_request_inner<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        if stream.len() < 4 {
            return Err(Error::ProtocolIncomplete);
        }

        // 解析multibulk count：*5\r\n
        let buf = stream.slice();
        let mut pos = 0;
        if buf.at(pos) as char != '*' {
            return Err(Error::RequestProtocolNotValid);
        }

        log::debug!(
            "+++ will parse req:{:?}",
            from_utf8(buf.to_vec().as_slice())
        );

        // 1 确认bulk数量；
        pos += 1;
        let len = buf.len();
        let (bulk_counto, meta_len) = parse_len(
            buf.sub_slice(pos, len - pos),
            "multibulk",
            ProtocolType::Request,
        )?;
        let bulk_count = match bulk_counto {
            None => 0,
            Some(c) => c,
        };
        pos += meta_len;
        if bulk_count > MAX_BULK_COUNT {
            log::warn!("found too long redis req with bulks/{}", bulk_count);
            return Err(Error::RequestProtocolNotValid);
        }

        if bulk_count < 1 {
            return Err(Error::RequestProtocolNotValid);
        }

        // 2 获得第一个bulk，即cmdname，得到协议的property；
        let cmd_bulk = next_bulk_fast(
            pos,
            buf.sub_slice(pos, len - pos),
            "cmdname",
            ProtocolType::Request,
        )?;
        if cmd_bulk.is_empty() {
            return Err(Error::RequestProtocolNotValid);
        }
        pos = cmd_bulk.data_end;
        let cmdname = cmd_bulk.bare_data(&buf);
        let prop = command::SUPPORTED.get_by_name(&cmdname)?;
        let last_key_idx = prop.last_key_index(bulk_count);

        // 先支持单key cmd
        // 如果没有key，或者key的个数为1，直接执行
        if prop.first_key_index() == 0
            || ((last_key_idx + 1 - prop.first_key_index()) / prop.key_step() == 1)
        {
            let mut key_count = 0;
            let hash;
            pos += 1;
            if prop.first_key_index() == 0 {
                debug_assert!(prop.operation().is_meta());
                use std::sync::atomic::{AtomicU64, Ordering};
                static RND: AtomicU64 = AtomicU64::new(0);
                hash = RND.fetch_add(1, Ordering::Relaxed) as i64;
                if bulk_count > 1 {
                    let skip_len = skip_common_bulks(
                        buf.sub_slice(pos, len - pos),
                        bulk_count - 1,
                        "meta-protocol",
                        ProtocolType::Request,
                    )?;
                    pos += skip_len;
                }
            } else {
                let skip_bulk_count = prop.first_key_index() - 1;
                if skip_bulk_count > 0 {
                    let skip_len = skip_common_bulks(
                        buf.sub_slice(pos, len - pos),
                        skip_bulk_count,
                        "skip-prefix",
                        ProtocolType::Request,
                    )?;
                    pos += skip_len;
                }
                pos += 1;
                let ktoken = next_bulk_fast(
                    pos,
                    buf.sub_slice(pos, len - pos),
                    "single-key",
                    ProtocolType::Request,
                )?;
                pos = ktoken.data_end;
                hash = alg.hash(&ktoken.bare_data(&buf));
                let skip_suffix_bulk_count = bulk_count - prop.first_key_index() - 1;
                if skip_suffix_bulk_count > 0 {
                    pos += 1;
                    let skip_len = skip_common_bulks(
                        buf.sub_slice(pos, len - pos),
                        skip_suffix_bulk_count,
                        "skip_suffix",
                        ProtocolType::Request,
                    )?;
                    pos += skip_len;
                }
                key_count = 1;
            }

            let reqdata = buf.sub_slice(0, pos);
            let guard = MemGuard(reqdata);
            // TODO: flag 还需要针对指令进行进一步设计
            let mut flag = Flag::from_mkey_op(false, prop.padding_rsp(), prop.operation().clone());
            if prop.noforward() {
                flag.set_noforward();
            }
            let cmd = HashedCommand::new(guard, hash, flag, key_count);

            // 处理完毕的字节需要take
            stream.take(pos);

            // process cmd
            process.process(cmd, true);
            return Ok(());
        } else {
            return Err(Error::ProtocolNotSupported);
        }
    }
}

impl Protocol for Redis {
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        let mut count = 0;
        loop {
            log::debug!("+++++ in parse");
            match self.parse_request_inner(stream, alg, process) {
                Ok(_) => count += 1,
                Err(e) => match e {
                    Error::ProtocolIncomplete => return Ok(()),
                    _ => return Err(e),
                },
            }
            if count > 1000 {
                log::warn!("too big pipeline: {}", count);
            }
        }
    }

    // 为每一个req解析一个response
    #[inline(always)]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        let response = data.slice();
        log::debug!(
            "+++ will parse rsp:{:?}",
            from_utf8(response.to_vec().as_slice())
        );
        // 响应目前只记录meta前缀长度
        let mut pos = 0;
        match response.at(0) as char {
            '*' => {
                pos += 1;
                let len = response.len();
                // multibulks count
                let (token_counto, meta_left_lenlen) = parse_len(
                    response.sub_slice(pos, len - pos),
                    "bulk",
                    ProtocolType::Response,
                )?;

                let token_count = match token_counto {
                    Some(c) => c,
                    None => 0,
                };
                pos += meta_left_lenlen;

                // 记录meta 长度
                debug_assert!(pos < 256);
                let mut flag = Flag::from_metalen_tokencount(pos as u8, token_count as u8);

                if token_count > 1 {
                    log::error!(
                        "found special resp with tokens/{}: {:?}",
                        token_count,
                        response
                    );
                    return Err(Error::ProtocolNotSupported);
                }

                // 解析并验证bulk tokens：$3\r\n123\r\n
                for i in 0..token_count {
                    if pos >= len {
                        return Err(Error::ProtocolIncomplete);
                    }
                    if response.at(pos) as char != '$' {
                        return Err(Error::ResponseProtocolNotValid);
                    }

                    pos += 1;
                    let (token_leno, meta_left_len) = parse_len(
                        response.sub_slice(pos, len - pos),
                        "bulk",
                        ProtocolType::Response,
                    )?;

                    let token_len = match token_leno {
                        Some(l) => l,
                        None => 0,
                    };
                    if token_len >= MAX_MSG_LEN {
                        log::warn!("careful too long token: {}", token_len);
                    }
                    // 走过$2\r\nab\r\n
                    pos += meta_left_len + token_len + token::REDIS_SPLIT_LEN;
                    if pos > len || (pos == len && i != token_count - 1) {
                        return Err(Error::ProtocolIncomplete);
                    }
                }

                flag.set_status_ok();
                // 到了这里，response已经解析完毕,对于resp，每个cmd并不知晓自己的key数量是0还是1
                return Ok(Some(Command::new(flag, 0, data.take(pos))));
            }
            '$' => {
                // one bulk
                pos += 1;
                let (leno, meta_left_len) = parse_len(
                    response.sub_slice(pos, response.len() - pos),
                    "rsp-bulk",
                    ProtocolType::Response,
                )?;
                let len = match leno {
                    Some(l) => l,
                    None => 0,
                };
                if len > MAX_MSG_LEN {
                    log::warn!("found too long respons/{}", len);
                }
                pos += meta_left_len + len;
                if len > 0 {
                    // 只有bare len大于0，才会有bare data + \r\n
                    pos += token::REDIS_SPLIT_LEN;
                }
                let mut flag = Flag::from_metalen_tokencount(0, 1u8);
                flag.set_status_ok();
                return Ok(Some(Command::new(flag, 0, data.take(pos))));
            }
            _ => {
                // others
                for i in 1..(response.len() - 1) {
                    if response.at(i) as char == '\r' && response.at(i + 1) as char == '\n' {
                        // i 为pos，+1 为len，再+1到下一个字符\n
                        // let rdata = response.sub_slice(0, i + 1 + 1);
                        let len = i + 1 + 1;
                        let mut flag = Flag::from_metalen_tokencount(0, 1u8);
                        flag.set_status_ok();
                        return Ok(Some(Command::new(flag, 0, data.take(len))));
                    }
                }
                return Err(Error::ProtocolIncomplete);
            }
        }
    }
    #[inline(always)]
    fn write_response<C: Commander, W: crate::ResponseWriter>(
        &self,
        ctx: &mut C,
        w: &mut W,
    ) -> Result<()> {
        // 首先确认request是否multi-key
        let key_count = ctx.request().key_count();
        let is_mkey_first = match key_count > 1 {
            true => ctx.request().is_mkey_first(),
            false => false,
        };

        // 如果是多个key的req，需要过滤掉每个resp的meta
        let resp = ctx.response();
        let mut oft = 0usize;
        if key_count > 1 {
            // 对于多个key，不管是不是第一个key对应的rsp，都需要去掉resp的meta前缀
            oft = resp.meta_len() as usize;
        }

        let len = resp.len() - oft;

        // 首先发送完整的meta
        // TODO: 1 如果有分片全部不可用，需要构建默认异常响应;
        // TODO: 2 特殊多key的响应 key_count 可能等于token数量？需要确认（理论上不应该存在） fishermen
        if is_mkey_first {
            let meta = format!("*{}\r\n", key_count);
            w.write(meta.as_bytes())?;
        }

        // 发送剩余rsp
        while oft < len {
            let data = resp.read(oft);
            log::debug!("+++++ in send redis");
            if data.len() < 1 {
                break;
            }
            w.write(data)?;
            oft += data.len();
        }
        Ok(())

        // 多个key，第一个response增加multi-bulk-len前缀，后面所有的response去掉bulk-len前缀
    }
    #[inline(always)]
    fn write_no_response<W: crate::ResponseWriter>(
        &self,
        req: &HashedCommand,
        w: &mut W,
    ) -> Result<()> {
        let rsp_idx = req.padding_rsp() as usize;
        debug_assert!(rsp_idx < PADDING_RSP_TABLE.len());
        let rsp = *PADDING_RSP_TABLE.get(rsp_idx).unwrap();
        w.write(rsp.as_bytes())?;
        Ok(())
    }
}

// 通过\r\n，快速定位一个bulk，一般一个bulk包含一个meta和一个data，eg:$2\r\nab\r\n, $2\r\n是meta，后面是data
// 特殊场景，一个bulk只有meta: $-1\r\n
// 返回值：协议长度不足或非法协议返回Err，否则返回Bulk，注意该bulk需要根据offset调整
fn next_bulk_fast(
    bulk_start: usize,
    data: RingSlice,
    name: &str,
    ptype: ProtocolType,
) -> Result<Bulk> {
    if data.len() <= 3 {
        return Err(Error::ProtocolIncomplete);
    }
    let len = data.len();
    let mut idx = 0;
    let mut meta_end_oft = 0;
    let mut data_end_oft = 0;
    let invalid_err = match ptype {
        ProtocolType::Request => Error::RequestProtocolNotValid,
        ProtocolType::Response => Error::ResponseProtocolNotValid,
    };

    idx += 1;
    if data.at(idx) as char != '$' {
        return Err(invalid_err);
    }

    while data.at(idx) as char != '\r' {
        let c = data.at(idx) as char;
        if c == '-' {
            // 处理$-1\r\n 这种情况
            idx += 1;
            while data.at(idx) as char != '\r' {
                idx += 1;
                // data里至少还应该有2个字节:\r\n
                if idx + 1 >= len {
                    return Err(Error::ProtocolIncomplete);
                }
            }
            meta_end_oft = idx + 1;
            data_end_oft = idx + 1;
            break;
        } else if c < '0' || c > '9' {
            log::warn!("found malformed len for {}", name);
            return Err(invalid_err);
        }
        // 持续轮询
        idx += 1;
        if idx + 1 >= len {
            return Err(Error::ProtocolIncomplete);
        }
    }

    idx += 1;
    if data.at(idx) as char != '\n' {
        return Err(invalid_err);
    }

    //  处理特殊bulk: $-1\r\n
    if meta_end_oft > 0 {
        return Ok(Bulk::from(bulk_start, meta_end_oft, data_end_oft));
    }

    meta_end_oft = idx;
    // 处理不同bulk
    idx += 1;
    if idx + 1 >= len {
        return Err(Error::ProtocolIncomplete);
    }

    while data.at(idx) as char != '\r' {
        idx += 1;
        if idx + 1 >= len {
            return Err(Error::ProtocolIncomplete);
        }
    }
    idx += 1;
    if data.at(idx) as char != '\n' {
        return Err(invalid_err);
    }
    data_end_oft = idx;
    return Ok(Bulk::from(bulk_start, meta_end_oft, data_end_oft));
}

// 根据\r\n 跳过n个bulk
fn skip_common_bulks(
    data: RingSlice,
    skip_bulk_count: usize,
    name: &str,
    ptype: ProtocolType,
) -> Result<usize> {
    if data.len() <= 2 {
        return Err(Error::ProtocolIncomplete);
    }
    let len = data.len();
    let mut idx = 0;
    let mut scount = 0;
    let invalid_err = match ptype {
        ProtocolType::Request => Error::RequestProtocolNotValid,
        ProtocolType::Response => Error::ResponseProtocolNotValid,
    };

    while scount < skip_bulk_count {
        // skip meta: $2\r\n
        if idx + 3 >= len {
            return Err(Error::ProtocolIncomplete);
        }
        if data.at(idx) as char != '$' {
            return Err(invalid_err);
        }
        idx += 1;
        if idx + 1 >= len {
            return Err(Error::ProtocolIncomplete);
        }
        while data.at(idx) as char != '\r' {
            idx += 1;
            if idx + 1 >= len {
                return Err(Error::ProtocolIncomplete);
            }
        }
        idx += 1;
        if data.at(idx) as char != '\n' {
            return Err(invalid_err);
        }

        // skip data: ab\r\n
        idx += 1;
        if idx + 1 >= len {
            return Err(Error::ProtocolIncomplete);
        }
        while data.at(idx) as char != '\r' {
            idx += 1;
            if idx + 1 >= len {
                return Err(Error::ProtocolIncomplete);
            }
        }
        idx += 1;
        if data.at(idx) as char != '\n' {
            return Err(invalid_err);
        }
    }
    Ok(idx)
}

// 解析bulk长度，起始位置是$2\r\n中的$的下一个元素，所以返回的元组中第二个长度比meta的实际长度小1
fn parse_len(data: RingSlice, name: &str, ptype: ProtocolType) -> Result<(Option<usize>, usize)> {
    if data.len() <= 2 {
        return Err(Error::ProtocolIncomplete);
    }
    let len = data.len();
    let mut idx = 0;
    let mut count = 0;
    let mut count_op = None;
    let invalid_err = match ptype {
        ProtocolType::Request => Error::RequestProtocolNotValid,
        ProtocolType::Response => Error::ResponseProtocolNotValid,
    };
    while data.at(idx) as char != '\r' {
        let c = data.at(idx) as char;
        if c == '-' {
            // 处理 $-1 这种情况
            idx += 1;
            while data.at(idx) as char != '\r' {
                idx += 1;
                if idx == len {
                    return Err(Error::ProtocolIncomplete);
                }
            }
            count_op = None;
            break;
        } else if c < '0' || c > '9' {
            log::warn!("found malformed len for {}", name);
            return Err(invalid_err);
        }
        count *= 10;
        count += c as usize - '0' as usize;
        idx += 1;
        if idx == len {
            return Err(Error::ProtocolIncomplete);
        }
        count_op = Some(count);
    }

    idx += 1;
    if data.at(idx) as char != '\n' {
        return Err(invalid_err);
    }
    // 长度包括flag和换行符，如"*123\r\n"是6，“$123\r\n”也是6
    Ok((count_op, idx + 1))
}

//
pub fn to_str(data: &Vec<u8>, ptype: ProtocolType) -> Result<&str> {
    let invalid_err = match ptype {
        ProtocolType::Request => Error::RequestProtocolNotValid,
        ProtocolType::Response => Error::ResponseProtocolNotValid,
    };

    match from_utf8(data.as_slice()) {
        Ok(s) => Ok(s),
        Err(_e) => Err(invalid_err),
    }
}
