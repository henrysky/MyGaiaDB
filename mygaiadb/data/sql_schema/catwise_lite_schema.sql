CREATE TABLE catwise (
    source_name character(21),
    source_id character(25),
    ra double precision,
    dec double precision,
    sigra double precision,
    sigdec double precision,
    sigradec double precision,
    w1snr real,
    w2snr real,
    w1mpro real,
    w1sigmpro real,
    w2mpro real,
    w2sigmpro real,
    nb smallint,
    na smallint,
    w1mag real,
    w1flg smallint,
    w2mag real,
    w2flg smallint,
    w1k real,
    w1mJDmean double precision,
    w2k real,
    w2mJDmean double precision,
    w1ab_map smallint,
    w2ab_map smallint,
    unwise_objid character(20),
    PRIMARY KEY (unwise_objid)
);
