use std::any::Any;
use std::convert::{TryFrom, TryInto};

use super::primitive::PrimitiveStatistics;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use parquet2::{
    schema::types::PhysicalType,
    statistics::{
        FixedLenStatistics as ParquetFixedLenStatistics, Statistics as ParquetStatistics,
    },
};

use super::Statistics;

/// Arrow-deserialized parquet Statistics of a fixed-len binary
#[derive(Debug, Clone, PartialEq)]
pub struct FixedLenStatistics {
    /// number of nulls
    pub null_count: Option<i64>,
    /// number of dictinct values
    pub distinct_count: Option<i64>,
    /// Minimum
    pub min_value: Option<Vec<u8>>,
    /// Maximum
    pub max_value: Option<Vec<u8>>,
    /// data type
    pub data_type: DataType,
}

impl Statistics for FixedLenStatistics {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

impl From<&ParquetFixedLenStatistics> for FixedLenStatistics {
    fn from(stats: &ParquetFixedLenStatistics) -> Self {
        let byte_lens = match stats.physical_type() {
            PhysicalType::FixedLenByteArray(size) => *size,
            _ => unreachable!(),
        };
        Self {
            null_count: stats.null_count,
            distinct_count: stats.distinct_count,
            min_value: stats.min_value.clone(),
            max_value: stats.max_value.clone(),
            data_type: DataType::FixedSizeBinary(byte_lens as usize),
        }
    }
}

impl TryFrom<(&ParquetFixedLenStatistics, DataType)> for PrimitiveStatistics<i128> {
    type Error = ArrowError;
    fn try_from((stats, data_type): (&ParquetFixedLenStatistics, DataType)) -> Result<Self> {
        let byte_lens = match stats.physical_type() {
            PhysicalType::FixedLenByteArray(size) => *size,
            _ => unreachable!(),
        };
        if byte_lens > 16 {
            Err(ArrowError::ExternalFormat(format!(
                "Can't deserialize i128 from Fixed Len Byte array with length {:?}",
                byte_lens
            )))
        } else {
            let paddings = (0..(16 - byte_lens)).map(|_| 0u8).collect::<Vec<_>>();
            let max_value = stats.max_value.as_ref().and_then(|value| {
                [paddings.as_slice(), value]
                    .concat()
                    .try_into()
                    .map(i128::from_be_bytes)
                    .ok()
            });

            let min_value = stats.min_value.as_ref().and_then(|value| {
                [paddings.as_slice(), value]
                    .concat()
                    .try_into()
                    .map(i128::from_be_bytes)
                    .ok()
            });
            Ok(Self {
                data_type,
                null_count: stats.null_count,
                distinct_count: stats.distinct_count,
                max_value,
                min_value,
            })
        }
    }
}

pub(super) fn statistics_from_fix_len(
    stats: &ParquetFixedLenStatistics,
    data_type: DataType,
) -> Result<Box<dyn Statistics>> {
    use DataType::*;
    Ok(match data_type {
        Decimal(_, _) => Box::new(PrimitiveStatistics::<i128>::try_from((stats, data_type))?),
        FixedSizeBinary(_) => Box::new(FixedLenStatistics::from(stats)),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Can't read {:?} from parquet",
                other
            )))
        }
    })
}
