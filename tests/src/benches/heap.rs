use criterion::{black_box, Criterion};
pub(super) fn bench_get_checked(c: &mut Criterion) {
    let slice = [1u8; 2048];
    let mut group = c.benchmark_group("vec_get");
    group.bench_function("checked", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0;
                for i in 0..slice.len() {
                    t += *slice.get(i).unwrap_or(&0) as u64;
                }
                t
            });
        });
    });
    group.bench_function("unchecked", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..slice.len() {
                    t += unsafe { *slice.get_unchecked(i) as u64 };
                }
                t
            });
        });
    });
    group.bench_function("iter", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0;
                for b in slice.iter() {
                    t += *b as u64;
                }
                t
            });
        });
    });
    group.bench_function("ptr", |b| {
        let ptr = slice.as_ptr();
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..slice.len() {
                    t += unsafe { *ptr.add(i) as u64 };
                }
                t
            });
        });
    });
    group.finish();
}
