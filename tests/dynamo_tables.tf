resource "aws_dynamodb_table" "nps_survey" {
    name           = "nps_survey"
    read_capacity  = "5"
    write_capacity = "5"
    hash_key       = "agency_id"
    range_key      = "profile_id"
    attribute {
      name = "agency_id"
      type = "N"
    }
    attribute {
      name = "profile_id"
      type = "N"
    }
}


resource "aws_dynamodb_table" "hash_only" {
    name           = "hash_only"
    read_capacity  = "5"
    write_capacity = "5"
    hash_key       = "agency_subdomain"
    global_secondary_index {
      name            = "HashOnlyExternalId"
      hash_key        = "external_id"
      read_capacity   = "15"
      write_capacity  = "15"
      projection_type = "ALL"
    }
    attribute {
      name = "agency_subdomain"
      type = "S"
    }
    attribute {
      name = "external_id"
      type = "S"
    }
}


resource "aws_dynamodb_table" "map_field" {
    name           = "map_field"
    read_capacity  = "5"
    write_capacity = "5"
    hash_key       = "agency_subdomain"
    attribute {
      name = "agency_subdomain"
      type = "S"
    }
}


resource "aws_dynamodb_table" "change_in_condition" {
    name                     = "change_in_condition"
    read_capacity            = "10"
    write_capacity           = "10"
    hash_key                 = "carelog_id"
    range_key                = "time"
    global_secondary_index {
      name            = "SavedInRDB"
      hash_key        = "saved_in_rdb"
      range_key       = "time"
      read_capacity   = "15"
      write_capacity  = "15"
      projection_type = "ALL"
    }
    local_secondary_index {
      name            = "SessionId"
      hash_key        = "carelog_id"
      range_key       = "session_id"
      projection_type = "ALL"
    }
    attribute {
      name = "carelog_id"
      type = "N"
    }
    attribute {
      name = "time"
      type = "N"
    }
    attribute {
      name = "saved_in_rdb"
      type = "N"
    }
    attribute {
      name = "session_id"
      type = "N"
    }
}
