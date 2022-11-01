//! # 已测试场景
//! ## 基本操作验证
//! - basic set
//! - set 过期时间后, ttl > 0
//! - mget 两个key
//! - mget 两个key, 其中只有一个set了, 预期应有一个none结果
//! - basic del
//! - basic incr
//! - 基础操作 decr, incrby, mset, exists, ttl, pttl, setnx, setex, expire, pexpire, expreat, pexpireat, persist
//! - hash基本操作, set 两个field后,hgetall
//! - hash基本操作, hmset 两个field后,hget
//! - hash基本操作hset, hsetnx, hmset, hincrby, hincrbyfloat, hdel, hget, hgetall, hlen, hkeys, hmget, hvals, hexists, hcan
//! - 地理位置相关 geoadd  geohash geopos geodist  
//!    georadius georadiusbymember存在问题
//! - set基本操作, sadd 1, 2, 3后, smembers
//! - list基本操作, lpush，rpush, rpushx, lpushx, linsert, lset, rpop, lpop, llen, lindex, lrange, ltrim, lrem
//! - 单个zset基本操作:
//!     zadd、zincrby、zrem、zremrangebyrank、zremrangebyscore、
//!     zremrangebylex、zrevrange、zcard、zrange、zrangebyscore、
//!     zrevrank、zrevrangebyscore、zrangebylex、zrevrangebylex、
//!     zcount、zlexcount、zscore、zscan
//! - set基本操作:
//!     sadd、smembers、srem、sismember、scard、spop、sscan
//! - list基本操作, rpush, llen, lpop, lrange, lset
//! - 单个zset基本操作, zadd, zrangebyscore withscore
//! - 单个long set基本操作, lsset, lsdump, lsput, lsgetall, lsdel, lslen, lsmexists, lsdset
//! - Bitmap基本操作:
//!     setbit、getbit、bitcount、bitpos、bitfield
//! - conn基本操作:
//!     ping、command、select、quit
//! - string基本操作:
//!     set、append、setrange、getrange、getset、strlen
//!## 复杂场景
//!  - set 1 1, ..., set 10000 10000等一万个key已由java sdk预先写入,
//! 从mesh读取, 验证业务写入与mesh读取之间的一致性
//! - value大小数组[4, 40, 400, 4000, 8000, 20000, 3000000],依次set后随机set,验证buffer扩容
//! - key大小数组[4, 40, 400, 4000], 依次set后get
//! - pipiline方式,set 两个key后,mget读取(注释了,暂未验证)

use crate::ci::env::*;
use crate::redis_helper::*;
use chrono::prelude::*;
use function_name::named;
use redis::{Commands, RedisError};
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use std::vec;
use std::{thread, time};

//获i64
fn find_primes(n: usize) -> Vec<i64> {
    let mut result = Vec::new();
    let mut is_prime = vec![true; n + 1];

    for i in 2..=n {
        if is_prime[i] {
            result.push(i as i64);
        }

        ((i * 2)..=n).into_iter().step_by(i).for_each(|x| {
            is_prime[x] = false;
        });
    }
    result
}

//基本场景
#[test]
fn test_args() {
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET")
        .arg("key1args")
        .arg(b"foo")
        .execute(&mut con);
    redis::cmd("SET")
        .arg(&["key2args", "bar"])
        .execute(&mut con);

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["key1args", "key2args"])
            .query(&mut con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

//基本set场景，key固定为foo或bar，value为简单数字或字符串
#[test]
fn test_basic_set() {
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET").arg("fooset").arg(42).execute(&mut con);
    assert_eq!(redis::cmd("GET").arg("fooset").query(&mut con), Ok(42));

    redis::cmd("SET").arg("barset").arg("foo").execute(&mut con);
    assert_eq!(
        redis::cmd("GET").arg("barset").query(&mut con),
        Ok(b"foo".to_vec())
    );
}

// set 过期时间后, ttl > 0
#[named]
#[test]
fn test_set_expire() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET")
        .arg(arykey)
        .arg(42usize)
        .arg("EX")
        .arg(3)
        .execute(&mut con);
    let ttl: usize = redis::cmd("TTL")
        .arg(arykey)
        .query(&mut con)
        .expect("ttl err");
    assert!(ttl > 0);
}

/// mget 两个key, 其中只有一个set了, 预期应有一个none结果
#[named]
#[test]
fn test_set_optionals() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET").arg(arykey).arg(1).execute(&mut con);

    let (a, b): (Option<i32>, Option<i32>) = redis::cmd("MGET")
        .arg(arykey)
        .arg("missing")
        .query(&mut con)
        .unwrap();
    assert_eq!(a, Some(1i32));
    assert_eq!(b, None);
}

#[named]
#[test]
fn test_basic_del() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET").arg(arykey).arg(42).execute(&mut con);

    assert_eq!(redis::cmd("GET").arg(arykey).query(&mut con), Ok(42));

    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(
        redis::cmd("GET").arg(arykey).query(&mut con),
        Ok(None::<usize>)
    );
}

#[named]
#[test]
fn test_basic_incr() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET").arg(arykey).arg(42).execute(&mut con);
    assert_eq!(redis::cmd("INCR").arg(arykey).query(&mut con), Ok(43usize));
}

/// hset 两个field后,hgetall
#[named]
#[test]
fn test_hash_hset_hgetall() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    redis::cmd("HSET")
        .arg(arykey)
        .arg("key_1")
        .arg(1)
        .execute(&mut con);
    redis::cmd("HSET")
        .arg(arykey)
        .arg("key_2")
        .arg(2)
        .execute(&mut con);

    let h: HashMap<String, i32> = redis::cmd("HGETALL")
        .arg(arykey)
        .query(&mut con)
        .expect("hgetall err");
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));
}

/// hmset 两个field后,hget
#[named]
#[test]
fn test_hash_hmset() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    redis::cmd("HMSET")
        .arg(arykey)
        .arg(&[("field_1", 42), ("field_2", 23)])
        .execute(&mut con);

    assert_eq!(
        redis::cmd("HGET")
            .arg(arykey)
            .arg("field_1")
            .query(&mut con),
        Ok(42)
    );
    assert_eq!(
        redis::cmd("HGET")
            .arg(arykey)
            .arg("field_2")
            .query(&mut con),
        Ok(23)
    );
}

///sadd 1, 2, 3后, smembers
#[named]
#[test]
fn test_set_ops() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.sadd(arykey, &[1, 2, 3]), Ok(3));

    let mut s: Vec<i32> = con.smembers(arykey).unwrap();
    s.sort_unstable();
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);
}

///list基本操作:
/// - lpush 插入四个 rpush插入4个
/// - lrange 0 -1 获取所有值
/// - lpop 弹出第一个
/// - rpop 弹出最后一个
/// - llen 长度
/// - lrange
/// - lset 将指定位置替换
/// - lindex 获取指定位置值
/// - linsert_before 在指定值之前插入
/// - linsert-after 之后插入
/// - lrange
/// - lrem 移除>0 从head起一个4
/// - lrem 移除<0 从tail 两个7
/// - lrem 移除=0  删除所有2
/// - lrange
/// - ltrim 保留指定区间
/// - lpushx 头插入一个
/// - rpushx 尾插入一个
/// - lrange
#[named]
#[test]
fn test_list_ops() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.lpush(arykey, &[1, 2]), Ok(2));
    assert_eq!(con.lpush(arykey, &[3, 4]), Ok(4));
    assert_eq!(con.rpush(arykey, &[5, 6]), Ok(6));
    assert_eq!(con.rpush(arykey, &[7, 8]), Ok(8));

    assert_eq!(con.lrange(arykey, 0, -1), Ok((4, 3, 2, 1, 5, 6, 7, 8)));

    assert_eq!(con.lpop(arykey, Default::default()), Ok(4));
    assert_eq!(con.rpop(arykey, Default::default()), Ok(8));
    assert_eq!(con.llen(arykey), Ok(6));
    assert_eq!(con.lrange(arykey, 0, -1), Ok((3, 2, 1, 5, 6, 7)));

    assert_eq!(con.lset(arykey, 0, 4), Ok(true));
    assert_eq!(con.lindex(arykey, 0), Ok(4));

    assert_eq!(con.linsert_before(arykey, 4, 4), Ok(7));
    assert_eq!(con.linsert_after(arykey, 7, 7), Ok(8));
    assert_eq!(con.lrange(arykey, 0, -1), Ok((4, 4, 2, 1, 5, 6, 7, 7)));

    assert_eq!(con.lrem(arykey, 1, 4), Ok(1));
    assert_eq!(con.lrem(arykey, -2, 7), Ok(2));
    assert_eq!(con.lrem(arykey, 0, 2), Ok(1));
    assert_eq!(con.lrange(arykey, 0, -1), Ok((4, 1, 5, 6)));

    assert_eq!(con.ltrim(arykey, 1, 2), Ok(true));
    assert_eq!(con.lpush_exists(arykey, 1), Ok(3));
    assert_eq!(con.rpush_exists(arykey, 5), Ok(4));
    assert_eq!(con.lrange(arykey, 0, -1), Ok((1, 1, 5, 5)));
}

/// - geoadd 添加地理位置经纬度
/// - geodist 获取km级别两个位置距离
/// - geopos 获取地理位置的坐标
/// - geohash：返回一个或多个位置对象的 geohash 值
/// - georadius 根据用户给定的经纬度坐标来获取100km内的地理位置集合,
///   WITHDIST: 在返回位置元素的同时， 将位置元素与中心之间的距离也一并返回,
//    WITHCOORD: 将位置元素的经度和纬度也一并返回。
/// - georadiusbymember 根据储存在位置集合里面的地点获取范围100km的地理位置集合
#[named]
#[test]
fn test_geo_ops() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(
        redis::cmd("GEOADD")
            .arg(arykey)
            .arg(30)
            .arg(50)
            .arg("Beijing")
            .arg(27)
            .arg(54)
            .arg("Tianjin")
            .arg(12)
            .arg(15)
            .arg("Hebei")
            .query(&mut con),
        Ok(3)
    );

    assert_eq!(
        redis::cmd("GEODIST")
            .arg(arykey)
            .arg("Beijing")
            .arg("Tianjin")
            .arg("km")
            .query(&mut con),
        Ok(489.9349)
    );

    assert_eq!(
        redis::cmd("GEOPOS")
            .arg(arykey)
            .arg("Beijing")
            .arg("Tianjin")
            .query(&mut con),
        Ok((
            (
                "30.00000089406967163".to_string(),
                "49.99999957172130394".to_string()
            ),
            (
                "26.99999839067459106".to_string(),
                "53.99999994301438733".to_string()
            ),
        ))
    );

    assert_eq!(
        redis::cmd("GEOHASH")
            .arg(arykey)
            .arg("Beijing")
            .arg("Tianjin")
            .query(&mut con),
        Ok(("u8vk6wjr4e0".to_string(), "u9e5nqkuc90".to_string()))
    );
    // operation not permitted on a read only server
    // assert_eq!(
    //     redis::cmd("GEORADIUS")
    //         .arg(arykey)
    //         .arg(28)
    //         .arg(30)
    //         .arg(100)
    //         .arg("km")
    //         .arg("WITHDIST")
    //         .arg("WITHCOORD")
    //         .query(&mut con),
    //     Ok(0)
    // );
    // assert_eq!(
    //     redis::cmd("GEORADIUSBYMEMBER")
    //         .arg(arykey)
    //         .arg("Tianjin")
    //         .arg(100)
    //         .arg("km")
    //         .query(&mut con),
    //     Ok(0)
    // );
}

/// - hash基本操作
/// - hmset test_hash_ops ("filed1", 1),("filed2", 2),("filed3", 3),("filed4", 4),("filed6", 6),
/// - hgetall test_hash_ops 获取该key下所有字段-值映射表
/// - hdel 删除字段filed1
/// - hlen 获取test_hash_ops表中字段数量 5
/// - hkeys 获取test_hash_ops表中所有字段
/// - hset 设置一个新字段 "filed5", 5 =》1
/// - hset 设置一个旧字段 "filed2", 22 =》0
/// - hmget获取字段filed2 filed5的值
/// - hincrby filed2 4 =>26
/// - hincrbyfloat filed5 4.4 =>8.4
/// - hexists 不存在的filed6=>0
/// - hsetnx不存在的 filed6 =》1
/// - hsetnx已经存在的 filed6 =》0
/// - hvals 获取所有test_hash_ops表filed字段的vals
///   当前filed2 22 filed3 3 filed4 4 filed 5 8.4 filede 6 6
///
/// - hsetnx不存在的 hash表hashkey =》1
/// - hget hash表hashkey hashfiled6=》6  
/// - hscan 获取hash表hashkey 所有的字段和value 的元组
/// - hscan_match 获取hash表test_hash_ops 和filed6相关的元组
#[named]
#[test]
fn test_hash_ops() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);
    redis::cmd("DEL").arg("hashkey").execute(&mut con);

    assert_eq!(
        con.hset_multiple(
            arykey,
            &[("filed1", 1), ("filed2", 2), ("filed3", 3), ("filed4", 4),]
        ),
        Ok(true)
    );
    assert_eq!(
        con.hgetall(arykey),
        Ok((
            "filed1".to_string(),
            1.to_string(),
            "filed2".to_string(),
            2.to_string(),
            "filed3".to_string(),
            3.to_string(),
            "filed4".to_string(),
            4.to_string(),
        ))
    );

    assert_eq!(con.hdel(arykey, "filed1"), Ok(1));
    assert_eq!(con.hlen(arykey), Ok(3));
    assert_eq!(
        con.hkeys(arykey),
        Ok((
            "filed2".to_string(),
            "filed3".to_string(),
            "filed4".to_string(),
        ))
    );

    assert_eq!(con.hset(arykey, "filed5", 5), Ok(1));
    assert_eq!(con.hset(arykey, "filed2", 22), Ok(0));
    assert_eq!(
        con.hget(arykey, &["filed2", "filed5"]),
        Ok((22.to_string(), 5.to_string()))
    );

    assert_eq!(con.hincr(arykey, "filed2", 4), Ok(26));
    assert_eq!(con.hincr(arykey, "filed5", 3.4), Ok(8.4));

    assert_eq!(con.hexists(arykey, "filed6"), Ok(0));
    assert_eq!(con.hset_nx(arykey, "filed6", 6), Ok(1));
    assert_eq!(con.hset_nx(arykey, "filed6", 6), Ok(0));

    assert_eq!(
        con.hvals(arykey),
        Ok((
            26.to_string(),
            3.to_string(),
            4.to_string(),
            8.4.to_string(),
            6.to_string()
        ))
    );

    assert_eq!(con.hset_nx("hashkey", "hashfiled6", 6), Ok(1));
    assert_eq!(con.hget("hashkey", "hashfiled6"), Ok(6.to_string()));

    let iter: redis::Iter<'_, (String, i64)> = con.hscan("hashkey").expect("hscan error");
    let mut found = HashSet::new();
    for item in iter {
        found.insert(item);
    }
    assert!(found.contains(&("hashfiled6".to_string(), 6)));

    let iter = con
        .hscan_match::<&str, &str, (String, i64)>(arykey, "filed6")
        .expect("hscan match error");
    let mut hscan_match_found = HashSet::new();
    for item in iter {
        hscan_match_found.insert(item);
    }
    assert!(hscan_match_found.contains(&("filed6".to_string(), 6)));
}

//getset key不存在时 返回nil 并set key
//get key =2
//getset key 1 返回旧值2
//get key =1
#[named]
#[test]
fn getset_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(
        con.getset::<&str, i32, Option<i32>>(arykey, 2)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err"),
        None
    );
    assert_eq!(con.get(arykey), Ok(2));
    assert_eq!(con.getset(arykey, 1), Ok(2));
    assert_eq!(con.get(arykey), Ok(1));
}

/// - mset ("xinxinkey1", 1), ("xinxinkey2", 2), ("xinxinkey3", 3),(arykey, 4)
/// - expire_at 设置xinxinkey1的过期时间为当前秒级别时间戳+2s
/// -  setex key2 4 22 将key2 value 改成22并设置过期时间4s
/// - ttl/pttl key3存在 但是没有设置过期时间=》-1
/// - ttl key1没过期
/// - sleep 2s
/// - pttl key2 没过期
/// - get value为22
/// - exists 检查key1不存在 因为已经过期 =》0
/// - ttl/pttl key1不存在时=》-2
/// - setnx key1 2 =>1 key1不存在（因为已经过期被删掉）才能setnx成功
/// - get key1 =>11
/// - setnx 已经存在的key3 =>0
/// - expire key3过期时间为1s
/// - incrby 4 =>8
/// - decrby 2 =>6
/// - decr =>5
/// - incrbyfloat 4.4 =>9.4
/// - pexpireat 设置arykey的过期时间为当前时间戳+2000ms p都是ms级别
/// - sleep 1s
/// - pexpire 设置arykey过期时间为5000ms
/// - ttl arykey过期时间》2000 由于pexpire把key3过期时间覆盖
/// - exists key3 0 已经过期
/// - persist arykey 移除过期时间
/// - ttl arykey -1 已经移除

#[named]
#[test]
fn string_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);
    redis::cmd("DEL").arg("xinxinkey1").execute(&mut con);
    redis::cmd("DEL").arg("xinxinkey2").execute(&mut con);
    redis::cmd("DEL").arg("xinxinkey3").execute(&mut con);
    assert_eq!(
        con.set_multiple(&[
            ("xinxinkey1", 1),
            ("xinxinkey2", 2),
            ("xinxinkey3", 3),
            (arykey, 4)
        ]),
        Ok(true)
    );

    let now = Local::now().timestamp();
    let pttl_now = Instant::now();
    assert_eq!(con.expire_at("xinxinkey1", (now + 2) as usize), Ok(1));
    assert_eq!(con.set_ex("xinxinkey2", 22, 4), Ok(true));

    assert_eq!(con.ttl("xinxinkey3"), Ok(-1));
    assert_eq!(con.pttl("xinxinkey3"), Ok(-1));

    assert_eq!(
        con.ttl("xinxinkey1"),
        Ok(4 - pttl_now.elapsed().as_secs() > 0)
    );

    thread::sleep(time::Duration::from_secs(2));

    assert_eq!(
        con.pttl("xinxinkey2"),
        Ok(4000 - pttl_now.elapsed().as_millis() > 0)
    );
    assert_eq!(con.get("xinxinkey2"), Ok(22));

    assert_eq!(con.exists("xinxinkey1"), Ok(0));
    assert_eq!(con.ttl("xinxinkey1"), Ok(-2));
    assert_eq!(con.pttl("xinxinkey1"), Ok(-2));

    assert_eq!(con.set_nx("xinxinkey1", 11), Ok(1));
    assert_eq!(con.get("xinxinkey1"), Ok(11));
    assert_eq!(con.set_nx("xinxinkey3", 2), Ok(0));

    assert_eq!(con.expire("xinxinkey3", 1), Ok(1));

    assert_eq!(con.incr(arykey, 4), Ok(8));
    assert_eq!(con.decr(arykey, 2), Ok(6));
    assert_eq!(redis::cmd("DECR").arg(arykey).query(&mut con), Ok(5));
    assert_eq!(con.incr(arykey, 4.4), Ok(9.4));

    assert_eq!(
        con.pexpire_at(arykey, (Local::now().timestamp_millis() + 2000) as usize),
        Ok(1)
    );
    thread::sleep(time::Duration::from_secs(1));
    assert_eq!(con.pexpire(arykey, 5000 as usize), Ok(1));
    assert_eq!(
        con.ttl(arykey),
        Ok(5000 - Instant::now().elapsed().as_millis() > 2000)
    );
    assert_eq!(con.exists("xinxinkey3"), Ok(0));

    assert_eq!(con.persist(arykey), Ok(1));
    assert_eq!(con.ttl(arykey), Ok(-1));
}

/// 单个long set基本操作, lsset, lsdump, lsput, lsgetall, lsdel, lslen, lsmexists, lsdset
#[named]
#[test]
fn test_lsset_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    // lsmalloc arykey 8, lsput argkey 1后的实际内存表现
    let lsset = vec![
        1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    redis::cmd("lsset")
        .arg(arykey)
        .arg(1)
        .arg(&lsset)
        .execute(&mut con);
    assert_eq!(
        redis::cmd("lsdump").arg(arykey).query(&mut con),
        Ok(lsset.clone())
    );

    assert_eq!(
        redis::cmd("lsput").arg(arykey).arg(2).query(&mut con),
        Ok(1)
    );
    assert_eq!(
        redis::cmd("lsgetall").arg(arykey).query(&mut con),
        Ok((1, 2))
    );

    assert_eq!(
        redis::cmd("lsdel").arg(arykey).arg(2).query(&mut con),
        Ok(1)
    );
    assert_eq!(redis::cmd("lslen").arg(arykey).query(&mut con), Ok(1));
    assert_eq!(
        redis::cmd("lsmexists")
            .arg(arykey)
            .arg(1)
            .arg(2)
            .query(&mut con),
        Ok("10".to_string())
    );

    let arykey = arykey.to_string() + "dset";
    redis::cmd("lsdset")
        .arg(&arykey)
        .arg(1)
        .arg(&lsset)
        .execute(&mut con);
    assert_eq!(
        redis::cmd("lsgetall").arg(&arykey).query(&mut con),
        Ok((1,))
    );
}

/// 单个zset基本操作:
/// zadd、zincrby、zrem、zremrangebyrank、zremrangebyscore、
/// zremrangebylex、zrevrange、zcard、zrange、zrangebyscore、
/// zrevrank、zrevrangebyscore、zrangebylex、zrevrangebylex、
/// zcount、zlexcount、zscore、zscan
#[named]
#[test]
fn zset_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    let values = &[
        (1, "one".to_string()),
        (2, "two".to_string()),
        (3, "three".to_string()),
        (4, "four".to_string()),
    ];

    assert_eq!(con.zadd_multiple(arykey, values), Ok(4));
    assert_eq!(
        con.zrange_withscores(arykey, 0, -1),
        Ok(vec![
            ("one".to_string(), 1),
            ("two".to_string(), 2),
            ("three".to_string(), 3),
            ("four".to_string(), 4),
        ])
    );

    assert_eq!(
        con.zrevrange_withscores(arykey, 0, -1),
        Ok(vec![
            ("four".to_string(), 4),
            ("three".to_string(), 3),
            ("two".to_string(), 2),
            ("one".to_string(), 1),
        ])
    );

    assert_eq!(con.zincr(arykey, "one", 4), Ok("5".to_string()));
    assert_eq!(con.zrem(arykey, "four"), Ok(1));
    assert_eq!(con.zremrangebyrank(arykey, 0, 0), Ok(1));
    assert_eq!(con.zrembyscore(arykey, 1, 3), Ok(1));

    let samescore = &[
        (0, "aaaa".to_string()),
        (0, "b".to_string()),
        (0, "c".to_string()),
        (0, "d".to_string()),
        (0, "e".to_string()),
    ];

    assert_eq!(con.zadd_multiple(arykey, samescore), Ok(5));
    assert_eq!(con.zrembylex(arykey, "[b", "(c"), Ok(1));
    assert_eq!(
        con.zrangebylex(arykey, "-", "(c"),
        Ok(vec!["aaaa".to_string(),])
    );
    assert_eq!(
        con.zrevrangebylex(arykey, "(c", "-"),
        Ok(vec!["aaaa".to_string(),])
    );
    assert_eq!(con.zcount(arykey, 0, 2), Ok(4));
    assert_eq!(con.zlexcount(arykey, "-", "+"), Ok(5));
    redis::cmd("DEL").arg(arykey).execute(&mut con);
    assert_eq!(con.zadd_multiple(arykey, values), Ok(4));

    assert_eq!(
        con.zrangebyscore(arykey, 0, 5),
        Ok(vec![
            "one".to_string(),
            "two".to_string(),
            "three".to_string(),
            "four".to_string(),
        ])
    );

    assert_eq!(
        con.zrevrangebyscore(arykey, 5, 0),
        Ok(vec![
            "four".to_string(),
            "three".to_string(),
            "two".to_string(),
            "one".to_string(),
        ])
    );

    assert_eq!(con.zcard(arykey), Ok(4));
    assert_eq!(con.zrank(arykey, "one"), Ok(0));
    assert_eq!(con.zscore(arykey, "one"), Ok(1));

    redis::cmd("DEL").arg(arykey).execute(&mut con);
    let values = &[(1, "one".to_string()), (2, "two".to_string())];
    assert_eq!(con.zadd_multiple(arykey, values), Ok(2));

    let res: Result<(i32, Vec<String>), RedisError> =
        redis::cmd("ZSCAN").arg(arykey).arg(0).query(&mut con);
    assert!(res.is_ok());
    let (cur, mut s): (i32, Vec<String>) = res.expect("ok");

    s.sort_unstable();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 4);
    assert_eq!(
        &s,
        &[
            "1".to_string(),
            "2".to_string(),
            "one".to_string(),
            "two".to_string()
        ]
    );
}

/// set基本操作:
/// sadd、smembers、srem、sismember、scard、spop、sscan
#[named]
#[test]
fn set_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.sadd(arykey, "one"), Ok(1));
    assert_eq!(con.sadd(arykey, "two"), Ok(1));

    assert_eq!(
        con.smembers(arykey),
        Ok(vec!["one".to_string(), "two".to_string(),])
    );

    assert_eq!(con.srem(arykey, "one"), Ok(1));
    assert_eq!(con.sismember(arykey, "one"), Ok(0));

    assert_eq!(con.scard(arykey), Ok(1));

    assert_eq!(con.srandmember(arykey), Ok("two".to_string()));
    assert_eq!(con.spop(arykey), Ok("two".to_string()));

    assert_eq!(con.sadd(arykey, "hello"), Ok(1));
    assert_eq!(con.sadd(arykey, "hi"), Ok(1));

    redis::cmd("DEL").arg("foo").execute(&mut con);
    assert_eq!(con.sadd("foo", &[1, 2, 3]), Ok(3));
    let res: Result<(i32, Vec<i32>), RedisError> =
        redis::cmd("SSCAN").arg("foo").arg(0).query(&mut con);
    assert!(res.is_ok());
    let (cur, mut s): (i32, Vec<i32>) = res.expect("ok");

    s.sort_unstable();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);
}

/// Bitmap基本操作:
/// setbit、getbit、bitcount、bitpos、bitfield
#[named]
#[test]
fn bit_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.setbit(arykey, 10086, true), Ok(0));
    assert_eq!(con.getbit(arykey, 10086), Ok(1));
    assert_eq!(con.bitcount(arykey), Ok(1));

    let res: Result<u64, RedisError> = redis::cmd("BITPOS")
        .arg(arykey)
        .arg(1)
        .arg(0)
        .query(&mut con);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), 10086);

    let res: Result<Vec<u8>, RedisError> = redis::cmd("BITFIELD")
        .arg(arykey)
        .arg("GET")
        .arg("u4")
        .arg("0")
        .query(&mut con);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), &[0u8]);
}

/// conn基本操作:
/// ping、command、select、quit
#[named]
#[test]
fn sys_basic() {
    // hello、master 未实现
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    let res: Result<String, RedisError> = redis::cmd("COMMAND").query(&mut con);
    assert_eq!(res.expect("ok"), "OK".to_string());
    let res: Result<String, RedisError> = redis::cmd("PING").query(&mut con);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), "PONG".to_string());

    let res: Result<String, RedisError> = redis::cmd("SELECT").arg(0).query(&mut con);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), "OK".to_string());

    assert_eq!(redis::cmd("quit").query(&mut con), Ok("OK".to_string()));
}

/// string基本操作:
/// set、append、setrange、getrange、getset、strlen
#[named]
#[test]
fn str_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.set(arykey, "Hello World"), Ok("OK".to_string()));
    assert_eq!(con.setrange(arykey, 6, "Redis"), Ok(11));
    assert_eq!(con.getrange(arykey, 6, 10), Ok("Redis".to_string()));
    assert_eq!(
        con.getset(arykey, "Hello World"),
        Ok("Hello Redis".to_string())
    );
    assert_eq!(con.append(arykey, "!"), Ok(12));
    assert_eq!(con.strlen(arykey), Ok(12));
}

//github ci 过不了,本地可以过,不清楚原因
/// pipiline方式,set 两个key后,mget读取
// #[test]
// fn test_pipeline() {
//     let mut con = get_conn(&file!().get_host());

//     let ((k1, k2),): ((i32, i32),) = redis::pipe()
//         .cmd("SET")
//         .arg("pipelinekey_1")
//         .arg(42)
//         .ignore()
//         .cmd("SET")
//         .arg("pipelinekey_2")
//         .arg(43)
//         .ignore()
//         .cmd("MGET")
//         .arg(&["pipelinekey_1", "pipelinekey_2"])
//         .query(&mut con)
//         .unwrap();

//     assert_eq!(k1, 42);
//     assert_eq!(k2, 43);
// }

/// set 1 1, ..., set 10000 10000等一万个key已由java sdk预先写入,
/// 从mesh读取, 验证业务写入与mesh读取之间的一致性
#[test]
fn test_get_write_by_sdk() {
    let mut con = get_conn(&file!().get_host());
    for i in exists_key_iter() {
        assert_eq!(redis::cmd("GET").arg(i).query(&mut con), Ok(i));
    }
}

///依次set [4, 40, 400, 4000, 8000, 20000, 3000000]大小的value
///验证buffer扩容,buffer初始容量4K,扩容每次扩容两倍
///后将[4, 40, 400, 4000, 8000, 20000, 3000000] shuffle后再依次set
///测试步骤:
///  1. set, key value size 4k以下，4次
///  3. set key value size 4k~8k，一次, buffer由4k扩容到8k
///  4. set key value size 8k~16k，一次，buffer在一次请求中扩容两次，由8k扩容到16k，16k扩容到32k，
///  5. set, key value size 2M以上，1次
///  6. 以上set请求乱序set一遍
#[named]
#[test]
fn test_set_value_fix_size() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    let mut v_sizes = [4, 40, 400, 4000, 8000, 20000, 3000000];
    for v_size in v_sizes {
        let val = vec![1u8; v_size];
        redis::cmd("SET").arg(arykey).arg(&val).execute(&mut con);
        assert_eq!(redis::cmd("GET").arg(arykey).query(&mut con), Ok(val));
    }

    //todo random iter
    use rand::seq::SliceRandom;
    let mut rng = rand::thread_rng();
    v_sizes.shuffle(&mut rng);
    for v_size in v_sizes {
        let val = vec![1u8; v_size];
        redis::cmd("SET").arg(arykey).arg(&val).execute(&mut con);
        assert_eq!(redis::cmd("GET").arg(arykey).query(&mut con), Ok(val));
    }
}

///依次set key长度为[4, 40, 400, 4000]
#[test]
fn test_set_key_fix_size() {
    let mut con = get_conn(&file!().get_host());

    let key_sizes = [4, 40, 400, 4000];
    for key_size in key_sizes {
        let key = vec![1u8; key_size];
        redis::cmd("SET").arg(&key).arg("foo").execute(&mut con);
        assert_eq!(
            redis::cmd("GET").arg(&key).query(&mut con),
            Ok("foo".to_string())
        );
    }
}

//mget 获取10000个key
#[test]
fn test_mget_10000() {
    let mut con = get_conn(&file!().get_host());

    let maxkey = 10000;
    let mut keys = Vec::with_capacity(maxkey);
    for i in 1..=maxkey {
        keys.push(i);
    }
    assert_eq!(redis::cmd("MGET").arg(&keys).query(&mut con), Ok(keys));
}
