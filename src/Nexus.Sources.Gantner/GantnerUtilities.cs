using Nexus.DataModel;
using UDBF.NET;

namespace Nexus.Sources;

internal static class GantnerUtilities
{
    public static NexusDataType GetNexusDataTypeFromUdbfDataType(UDBFDataType dataType)
    {
        return dataType switch
        {
            UDBFDataType.No => 0,
            UDBFDataType.Boolean => 0,
            UDBFDataType.SignedInt8 => NexusDataType.INT8,
            UDBFDataType.UnSignedInt8 => NexusDataType.UINT8,
            UDBFDataType.SignedInt16 => NexusDataType.INT16,
            UDBFDataType.UnSignedInt16 => NexusDataType.UINT16,
            UDBFDataType.SignedInt32 => NexusDataType.INT32,
            UDBFDataType.UnSignedInt32 => NexusDataType.UINT32,
            UDBFDataType.Float => NexusDataType.FLOAT32,
            UDBFDataType.BitSet8 => NexusDataType.UINT8,
            UDBFDataType.BitSet16 => NexusDataType.UINT16,
            UDBFDataType.BitSet32 => NexusDataType.UINT32,
            UDBFDataType.Double => NexusDataType.FLOAT64,
            UDBFDataType.SignedInt64 => NexusDataType.INT64,
            UDBFDataType.UnSignedInt64 => NexusDataType.UINT64,
            UDBFDataType.BitSet64 => NexusDataType.UINT64,
            _ => 0
        };
    }
}
