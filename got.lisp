(Struct
    (Field 'partitions'
        (GreedyRangeWithExceptionHandling
            (Struct
                (Field 'partition_header'
                    (Struct
                        (Field 'key_len'
                            (FormatField '>H')
                        )
                        (Field 'key'
                            (Bytes
                                (Path 'key_len')
                            )
                        )
                        (Field 'deletion_time'
                            (Struct
                                (Field 'local_deletion_time'
                                    (FormatField '>L')
                                )
                                (Field 'marked_for_delete_at'
                                    (FormatField '>Q')
                                )
                            )
                        )
                    )
                )
                (Field 'unfiltereds'
                    (RepeatUntil
                        (Function <function <lambda> at 0x7fb1243300d0>)
                        (Struct
                            (Field 'row_flags'
                                (Hex
                                    (FormatField '>B')
                                )
                            )
                            (Field 'row'
                                (IfThenElse
                                    (BinExpr ((this['row_flags'] and 1) == 0))
                                    (Struct
                                        (Field 'clustering_block'
                                            (IfThenElse
                                                (Function <function has_clustering_columns_func at 0x7fb124316ef0>)
                                                (Struct
                                                    (Field 'clustering_block_header'
                                                        (FormatField '>B')
                                                    )
                                                    (Field 'clustering_cells'
                                                        (Array
                                                            (Path '_root._.sstable_statistics.serialization_header.clustering_key_count')
                                                            (Struct
                                                                (Field 'key'
                                                                    (DynamicSwitch
                                                                        (Function <function get_clustering_key_type_func at 0x7fb124316dd0>)
                                                                        (Method <bound method Lark.parse of Lark(open('<string>'), parser='lalr', lexer='contextual', ...)>)
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                                (Pass)
                                            )
                                        )
                                        (Field 'serialized_row_body_size'
                                            (VarInt)
                                        )
                                        (Field 'row_body'
                                            (WithContext
                                                {'overridden_row_flags': <function <lambda> at 0x7fb124317d00>}
                                                (Struct
                                                    (Field 'row_body_start'
                                                        (Tell)
                                                    )
                                                    (Field 'previous_unfiltered_size'
                                                        (VarInt)
                                                    )
                                                    (Field 'timestamp_diff'
                                                        (VarInt)
                                                    )
                                                    (Field 'missing_columns'
                                                        (IfThenElse
                                                            (Function <function has_missing_columns_func at 0x7fb124315ea0>)
                                                            (EnabledColumns)
                                                            (Pass)
                                                        )
                                                    )
                                                    (Field 'cells'
                                                        (Array
                                                            (Function <function <lambda> at 0x7fb124317880>)
                                                            (Switch
                                                                (Function <function has_complex_deletion at 0x7fb124316200>)
                                                                (Case True
                                                                    (WithContext
                                                                        {'missing_columns': <function <lambda> at 0x7fb124317910>, 'cell_index': <function <lambda> at 0x7fb1243179a0>}
                                                                        (Struct
                                                                            (Field 'complex_deletion_time'
                                                                                (Struct
                                                                                    (Field 'delta_mark_for_delete_at'
                                                                                        (VarInt)
                                                                                    )
                                                                                    (Field 'delta_local_deletion_time'
                                                                                        (VarInt)
                                                                                    )
                                                                                )
                                                                            )
                                                                            (Field 'items_count'
                                                                                (VarInt)
                                                                            )
                                                                            (Field 'items'
                                                                                (Array
                                                                                    (Path 'items_count')
                                                                                    (WithContext
                                                                                        {'missing_columns': <function <lambda> at 0x7fb124316320>, 'cell_index': <function <lambda> at 0x7fb124316440>}
                                                                                        (Struct
                                                                                            (Field 'cell_flags'
                                                                                                (Hex
                                                                                                    (FormatField '>B')
                                                                                                )
                                                                                            )
                                                                                            (Field 'cell'
                                                                                                (IfThenElse
                                                                                                    (Function <function cell_has_non_empty_value at 0x7fb1245b7400>)
                                                                                                    (DynamicSwitch
                                                                                                        (Function <function get_cell_type_func at 0x7fb124316d40>)
                                                                                                        (Method <bound method Lark.parse of Lark(open('<string>'), parser='lalr', lexer='contextual', ...)>)
                                                                                                    )
                                                                                                    (Pass)
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                )
                                                                            )
                                                                        )
                                                                    ))
                                                                (Case False
                                                                    (WithContext
                                                                        {'missing_columns': <function <lambda> at 0x7fb124317a30>, 'cell_index': <function <lambda> at 0x7fb124317ac0>}
                                                                        (Struct
                                                                            (Field 'cell_flags'
                                                                                (Hex
                                                                                    (FormatField '>B')
                                                                                )
                                                                            )
                                                                            (Field 'cell'
                                                                                (IfThenElse
                                                                                    (Function <function cell_has_non_empty_value at 0x7fb1245b7400>)
                                                                                    (DynamicSwitch
                                                                                        (Function <function get_cell_type_func at 0x7fb124316d40>)
                                                                                        (Method <bound method Lark.parse of Lark(open('<string>'), parser='lalr', lexer='contextual', ...)>)
                                                                                    )
                                                                                    (Pass)
                                                                                )
                                                                            )
                                                                        )
                                                                    ))
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                    (Pass)
                                )
                            )
                        )
                    )
                )
            )
        )
    )
)
(Struct
    (Field 'metadata_count'
        (FormatField '>L')
    )
    (Field 'toc'
        (Array
            (Path 'metadata_count')
            (Struct
                (Field 'type'
                    (FormatField '>L')
                )
                (Field 'offset'
                    (FormatField '>L')
                )
            )
        )
    )
    (Field 'validation_metadata'
        (IfThenElse
            (Function <function metadata_exists.<locals>.fn at 0x7fb1243309d0>)
            (Struct
                (Field 'partition_name'
                    (Struct
                        (Field 'length'
                            (FormatField '>H')
                        )
                        (Field 'utf8_string'
                            (StringEncoded 'utf-8'
                                (Bytes
                                    (Path 'length')
                                )
                            )
                        )
                    )
                )
                (Field 'bloom_filter_fp_chance'
                    (FormatField '>d')
                )
            )
            (Pass)
        )
    )
    (Field 'compaction_metadata'
        (IfThenElse
            (Function <function metadata_exists.<locals>.fn at 0x7fb124330b80>)
            (Struct
                (Field 'length'
                    (FormatField '>L')
                )
                (Field 'bytes'
                    (Bytes
                        (Path 'length')
                    )
                )
            )
            (Pass)
        )
    )
    (Field 'statistics_metadata'
        (IfThenElse
            (Function <function metadata_exists.<locals>.fn at 0x7fb124330d30>)
            (Struct
                (Field 'parition_sizes'
                    (Struct
                        (Field 'length'
                            (FormatField '>L')
                        )
                        (Field 'bucket'
                            (Array
                                (Path 'length')
                                (Struct
                                    (Field 'prev_bucket_offset'
                                        (FormatField '>Q')
                                    )
                                    (Field 'name'
                                        (FormatField '>Q')
                                    )
                                )
                            )
                        )
                    )
                )
                (Field 'column_counts'
                    (Struct
                        (Field 'length'
                            (FormatField '>L')
                        )
                        (Field 'bucket'
                            (Array
                                (Path 'length')
                                (Struct
                                    (Field 'prev_bucket_offset'
                                        (FormatField '>Q')
                                    )
                                    (Field 'name'
                                        (FormatField '>Q')
                                    )
                                )
                            )
                        )
                    )
                )
                (Field 'commit_log_upper_bound'
                    (Struct
                        (Field 'segment_id'
                            (FormatField '>q')
                        )
                        (Field 'position_in_segment'
                            (FormatField '>L')
                        )
                    )
                )
                (Field 'min_timestamp'
                    (FormatField '>Q')
                )
                (Field 'max_timestamp'
                    (FormatField '>Q')
                )
                (Field 'min_local_deletion_time'
                    (Hex
                        (FormatField '>L')
                    )
                )
                (Field 'max_local_deletion_time'
                    (Hex
                        (FormatField '>L')
                    )
                )
                (Field 'min_ttl'
                    (FormatField '>L')
                )
                (Field 'max_ttl'
                    (FormatField '>L')
                )
                (Field 'compression_rate'
                    (FormatField '>d')
                )
                (Field 'tombstones'
                    (Struct
                        (Field 'bucket_number_limit'
                            (FormatField '>L')
                        )
                        (Field 'buckets'
                            (Struct
                                (Field 'length'
                                    (FormatField '>L')
                                )
                                (Field 'bucket'
                                    (Array
                                        (Path 'length')
                                        (Struct
                                            (Field 'prev_bucket_offset'
                                                (FormatField '>Q')
                                            )
                                            (Field 'name'
                                                (FormatField '>Q')
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                (Field 'level'
                    (FormatField '>L')
                )
                (Field 'repaired_at'
                    (FormatField '>Q')
                )
                (Field 'min_clustering_key'
                    (Struct
                        (Field 'length'
                            (FormatField '>L')
                        )
                        (Field 'column'
                            (Array
                                (Path 'length')
                                (Struct
                                    (Field 'length'
                                        (FormatField '>H')
                                    )
                                    (Field 'name'
                                        (Bytes
                                            (Path 'length')
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                (Field 'max_clustering_key'
                    (Struct
                        (Field 'length'
                            (FormatField '>L')
                        )
                        (Field 'column'
                            (Array
                                (Path 'length')
                                (Struct
                                    (Field 'length'
                                        (FormatField '>H')
                                    )
                                    (Field 'name'
                                        (Bytes
                                            (Path 'length')
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                (Field 'has_legacy_counters'
                    (FormatField '>B')
                )
                (Field 'number_of_columns'
                    (FormatField '>Q')
                )
                (Field 'number_of_rows'
                    (FormatField '>Q')
                )
                (Field 'commit_log_lower_bound'
                    (Struct
                        (Field 'segment_id'
                            (FormatField '>q')
                        )
                        (Field 'position_in_segment'
                            (FormatField '>L')
                        )
                    )
                )
                (Field 'commit_log_intervals_length'
                    (FormatField '>L')
                )
                (Field 'commit_log_intervals'
                    (Array
                        (Path 'commit_log_intervals_length')
                        (Struct
                            (Field 'start'
                                (Struct
                                    (Field 'segment_id'
                                        (FormatField '>q')
                                    )
                                    (Field 'position_in_segment'
                                        (FormatField '>L')
                                    )
                                )
                            )
                            (Field 'end'
                                (Struct
                                    (Field 'segment_id'
                                        (FormatField '>q')
                                    )
                                    (Field 'position_in_segment'
                                        (FormatField '>L')
                                    )
                                )
                            )
                        )
                    )
                )
                (Field 'TODO_WHY_IS_THIS_NEEDED'
                    (Bytes
                        (Int 1)
                    )
                )
                (Field 'host_id'
                    (Hex
                        (Bytes
                            (Int 16)
                        )
                    )
                )
            )
            (Pass)
        )
    )
    (Field 'serialization_header'
        (IfThenElse
            (Function <function metadata_exists.<locals>.fn at 0x7fb124330ee0>)
            (Struct
                (Field 'min_timestamp'
                    (VarInt)
                )
                (Field 'min_local_deletion_time'
                    (VarInt)
                )
                (Field 'min_ttl'
                    (VarInt)
                )
                (Field 'partition_key_type'
                    (Struct
                        (Field 'name_length'
                            (VarInt)
                        )
                        (Field 'name'
                            (StringEncoded 'ascii'
                                (Bytes
                                    (Path 'name_length')
                                )
                            )
                        )
                    )
                )
                (Field 'clustering_key_count'
                    (VarInt)
                )
                (Field 'clustering_key_types'
                    (Array
                        (Path 'clustering_key_count')
                        (Struct
                            (Field 'name_length'
                                (VarInt)
                            )
                            (Field 'name'
                                (StringEncoded 'ascii'
                                    (Bytes
                                        (Path 'name_length')
                                    )
                                )
                            )
                        )
                    )
                )
                (Field 'static_column_count'
                    (VarInt)
                )
                (Field 'static_columns'
                    (Array
                        (Path 'static_column_count')
                        (Struct
                            (Field 'name_length'
                                (FormatField '>B')
                            )
                            (Field 'name'
                                (StringEncoded 'ascii'
                                    (Bytes
                                        (Path 'name_length')
                                    )
                                )
                            )
                            (Field 'type'
                                (Struct
                                    (Field 'name_length'
                                        (VarInt)
                                    )
                                    (Field 'name'
                                        (StringEncoded 'ascii'
                                            (Bytes
                                                (Path 'name_length')
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
                (Field 'regular_column_count'
                    (VarInt)
                )
                (Field 'regular_columns'
                    (Array
                        (Path 'regular_column_count')
                        (Struct
                            (Field 'name_length'
                                (FormatField '>B')
                            )
                            (Field 'name'
                                (StringEncoded 'ascii'
                                    (Bytes
                                        (Path 'name_length')
                                    )
                                )
                            )
                            (Field 'type'
                                (Struct
                                    (Field 'name_length'
                                        (VarInt)
                                    )
                                    (Field 'name'
                                        (StringEncoded 'ascii'
                                            (Bytes
                                                (Path 'name_length')
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
            (Pass)
        )
    )
)
(Struct
    (Field 'version'
        (Hex
            (FormatField '>B')
        )
    )
    (Field 'flags'
        (Hex
            (FormatField '>B')
        )
    )
    (Field 'stream'
        (FormatField '>H')
    )
    (Field 'opcode'
        (Hex
            (FormatField '>B')
        )
    )
    (Field 'length'
        (FormatField '>L')
    )
    (Field 'body'
        (Switch
            (Path 'opcode')
            (Case 0
                (Struct
                    (Field 'code'
                        (FormatField '>L')
                    )
                    (Field 'length'
                        (FormatField '>H')
                    )
                    (Field 'message'
                        (StringEncoded 'utf-8'
                            (Bytes
                                (Path 'length')
                            )
                        )
                    )
                ))
            (Case 7
                (Struct
                    (Field 'query'
                        (Struct
                            (Field 'length'
                                (FormatField '>L')
                            )
                            (Field 'string'
                                (StringEncoded 'utf-8'
                                    (Bytes
                                        (Path 'length')
                                    )
                                )
                            )
                        )
                    )
                    (Field 'consistency'
                        (FormatField '>H')
                    )
                    (Field 'flags'
                        (FormatField '>B')
                    )
                ))
            (Case 8
                (Struct
                    (Field 'kind'
                        (FormatField '>L')
                    )
                    (Field 'result'
                        (Switch
                            (Path 'kind')
                            (Case 1
                                (Bytes
                                    (Int 0)
                                ))
                            (Case 2
                                (Struct
                                    (Field 'metadata'
                                        (Struct
                                            (Field 'flags'
                                                (FormatField '>L')
                                            )
                                            (Field 'columns_count'
                                                (FormatField '>L')
                                            )
                                            (Field 'paging_state'
                                                (IfThenElse
                                                    (Function <function <lambda> at 0x7fb1245b5ea0>)
                                                    (Struct
                                                        (Field 'length'
                                                            (FormatField '>L')
                                                        )
                                                        (Field 'string'
                                                            (StringEncoded 'utf-8'
                                                                (Bytes
                                                                    (Path 'length')
                                                                )
                                                            )
                                                        )
                                                    )
                                                    (Pass)
                                                )
                                            )
                                            (Field 'global_table_spec'
                                                (IfThenElse
                                                    (Function <function <lambda> at 0x7fb1245b6200>)
                                                    (Struct
                                                        (Field 'keyspace'
                                                            (Struct
                                                                (Field 'length'
                                                                    (FormatField '>H')
                                                                )
                                                                (Field 'string'
                                                                    (StringEncoded 'utf-8'
                                                                        (Bytes
                                                                            (Path 'length')
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                        (Field 'table'
                                                            (Struct
                                                                (Field 'length'
                                                                    (FormatField '>H')
                                                                )
                                                                (Field 'string'
                                                                    (StringEncoded 'utf-8'
                                                                        (Bytes
                                                                            (Path 'length')
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                    (Pass)
                                                )
                                            )
                                            (Field 'column_specs'
                                                (IfThenElse
                                                    (Function <function <lambda> at 0x7fb1245b63b0>)
                                                    (Array
                                                        (Path 'columns_count')
                                                        (Struct
                                                            (Field 'keyspace'
                                                                (IfThenElse
                                                                    (Function <function <lambda> at 0x7fb1245b6710>)
                                                                    (Struct
                                                                        (Field 'length'
                                                                            (FormatField '>H')
                                                                        )
                                                                        (Field 'string'
                                                                            (StringEncoded 'utf-8'
                                                                                (Bytes
                                                                                    (Path 'length')
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                    (Pass)
                                                                )
                                                            )
                                                            (Field 'table'
                                                                (IfThenElse
                                                                    (Function <function <lambda> at 0x7fb1245b6680>)
                                                                    (Struct
                                                                        (Field 'length'
                                                                            (FormatField '>H')
                                                                        )
                                                                        (Field 'string'
                                                                            (StringEncoded 'utf-8'
                                                                                (Bytes
                                                                                    (Path 'length')
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                    (Pass)
                                                                )
                                                            )
                                                            (Field 'name'
                                                                (Struct
                                                                    (Field 'length'
                                                                        (FormatField '>H')
                                                                    )
                                                                    (Field 'string'
                                                                        (StringEncoded 'utf-8'
                                                                            (Bytes
                                                                                (Path 'length')
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                            (Field 'type'
                                                                (Struct
                                                                    (Field 'id'
                                                                        (FormatField '>H')
                                                                    )
                                                                    (Field 'value'
                                                                        (DynamicSwitch
                                                                            (Function <function <lambda> at 0x7fb1245b5d80>)
                                                                            (Function <function <lambda> at 0x7fb1245b5f30>)
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                    (Pass)
                                                )
                                            )
                                        )
                                    )
                                    (Field 'rows_count'
                                        (FormatField '>L')
                                    )
                                    (Field 'rows_content'
                                        (Array
                                            (Path 'rows_count')
                                            (Struct
                                                (Field 'row'
                                                    (Array
                                                        (Path '_.metadata.columns_count')
                                                        (Struct
                                                            (Field 'column_length'
                                                                (FormatField '>l')
                                                            )
                                                            (Field 'column_value'
                                                                (IfThenElse
                                                                    (Function <function <lambda> at 0x7fb1245b6b90>)
                                                                    (Bytes
                                                                        (Path 'column_length')
                                                                    )
                                                                    (Pass)
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                ))
                        )
                    )
                ))
        )
    )
)
