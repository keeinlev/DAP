resource "aws_glue_catalog_database" "events" {
  name = "dap_events_${terraform.workspace}"
}

resource "aws_glue_catalog_table" "events" {
  name          = "events_${terraform.workspace}"
  database_name = aws_glue_catalog_database.events.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    columns {
      name = "event_id"
      type = "string"
    }

    columns {
      name = "event_type"
      type = "string"
    }

    columns {
      name = "event_version"
      type = "int"
    }

    columns {
      name = "timestamp"
      type = "bigint"
    }

    columns {
      name = "payload"
      type = "string"
    }
  }
}