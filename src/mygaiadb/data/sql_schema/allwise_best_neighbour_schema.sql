CREATE TABLE allwise_best_neighbour (
    source_id bigint,
    original_ext_source_id varchar,
    angular_distance real,
    xm_flag smallint,
    allwise_oid bigint,
    number_of_neighbours smallint,
    number_of_mates smallint,
    PRIMARY KEY (source_id, original_ext_source_id)
);
