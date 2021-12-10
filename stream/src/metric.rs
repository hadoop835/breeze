use metrics::{Metric, Path};
use protocol::{Operation, OPS};
macro_rules! define_metrics {
    (qps:$($qps:ident),+;count:$($count:ident),+) => {
        pub struct CbMetrics {
            $(
                $qps: Metric,
            )+
            $(
                $count: Metric,
            )+
            ops: [Metric; OPS.len()],
        }
        impl CbMetrics {
            $(
            #[inline(always)]
            pub fn $qps(&mut self) -> &mut Metric {
                &mut self.$qps
            }
            )+
            $(
            #[inline(always)]
            pub fn $count(&mut self) -> &mut Metric {
                &mut self.$count
            }
            )+
            #[inline(always)]
            pub fn ops(&mut self, op:&Operation) -> &mut Metric {
                debug_assert!(op.id() < self.ops.len());
                unsafe{self.ops.get_unchecked_mut(op.id()) }
            }
            pub fn new(path:&Path) -> Self {
                let ops: [Metric; OPS.len()] =
                    array_init::array_init(|idx| path.rtt(OPS[idx].name()));
                $(
                let $qps = path.qps(stringify!($qps));
                )+
                $(
                let $count = path.count(stringify!($count));
                )+
                Self {
                    ops,
                    $(
                        $qps,
                    )+
                    $(
                        $count,
                    )+
                }
            }
        }
    };
}

define_metrics!(qps: tx, rx, err, cps, kps, conn; count:conn_num);
