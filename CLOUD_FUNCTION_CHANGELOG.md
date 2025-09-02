# Cloud Function Version History

## Version 2.0 - Enhanced Multi-Tenant Processing (Current)

### Major Features Added

- **Tenant Intelligence**: Automatically parses file paths to extract tenant information and data types
- **File Type Detection**: Added support for 10+ file formats including CSV, JSON, Parquet, Excel, pickle, and text files
- **Regional Awareness**: Extracts region information from bucket names for proper geographic processing
- **Correlation Tracking**: Implements UUID-based correlation IDs for end-to-end request tracing
- **Enhanced Message Structure**: Rich metadata including tenant context, file characteristics, and routing hints

### Message Structure Evolution

The message payload sent to Pub/Sub has been significantly enhanced:

**Previous Version**:

```json
{
  "file_name": "data.csv",
  "bucket_name": "my-bucket",
  "file_size": 1024
}
```

**Current Version**:

```json
{
  "file_name": "tenants/tenant-001/data/raw/sales.csv",
  "bucket_name": "terraops-us-central1-tenant-data",
  "file_size": 1048576,
  "tenant_id": "tenant-001",
  "data_type": "raw",
  "folder_category": "tenant-data",
  "region": "us-central1",
  "file_info": {
    "file_type": "csv",
    "category": "data",
    "is_supported": true,
    "file_name_without_ext": "sales"
  },
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
  "routing_hints": {
    "requires_processing": true,
    "priority": "high",
    "engine_suggestion": "auto"
  },
  "processing_timestamp": "2024-08-29T10:30:00Z",
  "function_version": "2.0-enhanced"
}
```

### Tenant Path Analysis

Added intelligent parsing for different tenant structures:

- **Shared Resources**: `shared/ml-models/` and `shared/reference-data/`
- **Business Domains**: `tenants/business-domains/` for schema configurations
- **Tenant-Specific Data**: `tenants/tenant-XXX/data/` with raw, processed, and bad-records folders

### File Type Support

Extended file format support with categorization:

- **Data Files**: CSV, JSON, JSONL, Parquet, Excel (XLSX/XLS)
- **Model Files**: Pickle files (PKL/PICKLE)
- **Reference Files**: JSON, CSV, TXT for lookup data
- **Log Files**: LOG, TXT for system logs

### Error Handling Improvements

- **Dead Letter Queue Integration**: Failed messages automatically routed to DLQ topic
- **Structured Error Logging**: Rich error context with tenant and file information
- **Retry Logic**: Automatic retry attempts with correlation tracking
- **Fallback Processing**: Graceful degradation when components fail

### Regional Deployment

- **Multi-Region Support**: Single function monitors 12 regions across 4 continents
- **Bucket Intelligence**: Automatically detects region from bucket naming convention
- **Cost Optimization**: Centralized monitoring reduces infrastructure overhead

### Performance Optimizations

- **Memory Allocation**: Increased to 256MB for enhanced message processing
- **Structured Logging**: Reduced logging overhead with consistent format
- **Message Validation**: Early validation prevents downstream processing errors

## Version 1.0 - Basic File Processing (Previous)

### Core Features

- Basic file upload detection
- Simple Pub/Sub message publishing
- Single region support (us-west1)
- Minimal message structure
- Basic error handling

### Limitations of Previous Version

- No tenant awareness or isolation
- Limited file type support
- No correlation tracking
- Single region deployment only
- Basic error messages without context

## Migration Notes

### Breaking Changes from v1.0 to v2.0

- Message structure completely redesigned
- Additional environment variables required (DEAD_LETTER_TOPIC)
- Downstream components must handle enhanced message format

### Compatibility

- Router Function v2.0 required for enhanced message processing
- Processing engines updated to handle new correlation tracking
- Terraform configurations updated for multi-region deployment

## Deployment Status

- **Current Deployment**: Production across 12 regions
- **Monitoring**: Centralized function monitoring all regional buckets
- **Status**: Active and processing files with enhanced intelligence

## Future Enhancements

- Additional file format support as requirements evolve
- Enhanced tenant analytics and processing insights
- Advanced routing hints for specialized processing needs
- Integration with additional downstream processing systems
