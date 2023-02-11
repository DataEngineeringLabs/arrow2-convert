use arrow2::{array::Array, buffer::Buffer};
use arrow2_convert::{
    deserialize::TryIntoCollection, serialize::TryIntoArrow, ArrowDeserialize, ArrowField,
    ArrowSerialize,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[derive(ArrowField, ArrowSerialize, ArrowDeserialize)]
#[arrow_field(transparent)]
pub struct BufStruct(Buffer<u16>);

#[derive(ArrowField, ArrowSerialize, ArrowDeserialize)]
#[arrow_field(transparent)]
pub struct VecStruct(Vec<u16>);

pub fn bench_buffer_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize");
    for size in [1, 10, 100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("Buffer", size), size, |b, &size| {
            let data = [BufStruct((0..size as u16).into_iter().collect())];
            b.iter(|| {
                let _: Box<dyn Array> = TryIntoArrow::try_into_arrow(black_box(&data)).unwrap();
            });
        });
        group.bench_with_input(BenchmarkId::new("Vec", size), size, |b, &size| {
            let data = [VecStruct((0..size as u16).into_iter().collect())];
            b.iter(|| {
                let _: Box<dyn Array> = TryIntoArrow::try_into_arrow(black_box(&data)).unwrap();
            });
        });
    }
}
pub fn bench_buffer_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("deserialize");
    for size in [1, 10, 100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("Buffer", size), size, |b, &size| {
            let data: Box<dyn Array> = [BufStruct((0..size as u16).into_iter().collect())]
                .try_into_arrow()
                .unwrap();
            b.iter_batched(
                || data.clone(),
                |data| {
                    let _: Vec<BufStruct> =
                        TryIntoCollection::try_into_collection(black_box(data)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            )
        });
        group.bench_with_input(BenchmarkId::new("Vec", size), size, |b, &size| {
            let data: Box<dyn Array> = [VecStruct((0..size as u16).into_iter().collect())]
                .try_into_arrow()
                .unwrap();
            b.iter_batched(
                || data.clone(),
                |data| {
                    let _: Vec<VecStruct> =
                        TryIntoCollection::try_into_collection(black_box(data)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(benches, bench_buffer_serialize, bench_buffer_deserialize);
criterion_main!(benches);
