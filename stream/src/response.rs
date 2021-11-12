use protocol::proto::Command;

pub struct Response {
    cmd: Command,
}
impl Response {
    #[inline(always)]
    pub fn from(cmd: Command) -> Self {
        Self { cmd }
    }
}
//impl protocol::resp::Response for Response {
//    fn ok(&self) -> bool {
//        todo!();
//    }
//}

impl std::ops::Deref for Response {
    type Target = Command;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.cmd
    }
}

impl AsRef<Command> for Response {
    #[inline(always)]
    fn as_ref(&self) -> &Command {
        &self.cmd
    }
}
