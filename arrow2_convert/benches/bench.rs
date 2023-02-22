use arrow2::{array::Array, buffer::Buffer};
use arrow2_convert::{
    deserialize::TryIntoCollection, serialize::TryIntoArrow, ArrowDeserialize, ArrowField,
    ArrowSerialize,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// Arrow stores U8 arrays as `arrow2::array::BinaryArray`
#[derive(ArrowField, ArrowSerialize, ArrowDeserialize)]
#[arrow_field(transparent)]
pub struct BufU8Struct(Buffer<u8>);

// Arrow stores other arrows as `arrow2::array::ListArray`
#[derive(ArrowField, ArrowSerialize, ArrowDeserialize)]
#[arrow_field(transparent)]
pub struct BufU32Struct(Buffer<u32>);

// Arrow stores U8 arrows as `arrow2::array::BinaryArray`
#[derive(ArrowField, ArrowSerialize, ArrowDeserialize)]
#[arrow_field(transparent)]
pub struct VecU8Struct(Vec<u8>);

// Arrow stores other arrows as `arrow2::array::ListArray`
#[derive(ArrowField, ArrowSerialize, ArrowDeserialize)]
#[arrow_field(transparent)]
pub struct VecU32Struct(Vec<u32>);

pub fn bench_buffer_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize");
    for size in [1, 10, 100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("BufferU8", size), size, |b, &size| {
            let data = [BufU8Struct((0..size as u8).into_iter().collect())];
            b.iter(|| {
                let _: Box<dyn Array> = TryIntoArrow::try_into_arrow(black_box(&data)).unwrap();
            });
        });
        group.bench_with_input(BenchmarkId::new("VecU8", size), size, |b, &size| {
            let data = [VecU8Struct((0..size as u8).into_iter().collect())];
            b.iter(|| {
                let _: Box<dyn Array> = TryIntoArrow::try_into_arrow(black_box(&data)).unwrap();
            });
        });
        group.bench_with_input(BenchmarkId::new("BufferU32", size), size, |b, &size| {
            let data = [BufU32Struct((0..size as u32).into_iter().collect())];
            b.iter(|| {
                let _: Box<dyn Array> = TryIntoArrow::try_into_arrow(black_box(&data)).unwrap();
            });
        });
        group.bench_with_input(BenchmarkId::new("VecU32", size), size, |b, &size| {
            let data = [VecU32Struct((0..size as u32).into_iter().collect())];
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
        group.bench_with_input(BenchmarkId::new("BufferU8", size), size, |b, &size| {
            let data: Box<dyn Array> = [BufU8Struct((0..size as u8).into_iter().collect())]
                .try_into_arrow()
                .unwrap();
            b.iter_batched(
                || data.clone(),
                |data| {
                    let _: Vec<BufU8Struct> =
                        TryIntoCollection::try_into_collection(black_box(data)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            )

        });
        group.bench_with_input(BenchmarkId::new("VecU8", size), size, |b, &size| {
            let data: Box<dyn Array> = [VecU8Struct((0..size as u8).into_iter().collect())]
                .try_into_arrow()
                .unwrap();
            b.iter_batched(
                || data.clone(),
                |data| {
                    let _: Vec<VecU8Struct> =
                        TryIntoCollection::try_into_collection(black_box(data)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
        group.bench_with_input(BenchmarkId::new("BufferU32", size), size, |b, &size| {
            let data: Box<dyn Array> = [BufU32Struct((0..size as u32).into_iter().collect())]
                .try_into_arrow()
                .unwrap();
            b.iter_batched(
                || data.clone(),
                |data| {
                    let _: Vec<BufU32Struct> =
                        TryIntoCollection::try_into_collection(black_box(data)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            )
        });
        group.bench_with_input(BenchmarkId::new("VecU32", size), size, |b, &size| {
            let data: Box<dyn Array> = [VecU32Struct((0..size as u32).into_iter().collect())]
                .try_into_arrow()
                .unwrap();
            b.iter_batched(
                || data.clone(),
                |data| {
                    let _: Vec<VecU32Struct> =
                        TryIntoCollection::try_into_collection(black_box(data)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(benches, bench_buffer_serialize, bench_buffer_deserialize);
criterion_main!(benches);
