# Pipette Documentation

DOCUMENT UNDER CONSTRUCTION

## Concepts


### Function


### Streams


### Specification

Part 1: Config

Configs are defined in YAML.

```yaml
version: 2

config:
  io_unit:
    database:
      # Identifier [ database & schema & table]
      database: reporting
      schema: rptsvcacct
      target: io_unit
      # Strategy [ explicit | explicit-json-overflow | json | text | csv-array ]
      storage-strategy: json
```

### Considerations

