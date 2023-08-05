CREATE TABLE allwise (
    designation character(20),
    ra double precision,
    dec double precision,
    sigra double precision,
    sigdec double precision,
    sigradec double precision,
    w1mpro real,
    w1sigmpro real,
    w1snr real,
    w2mpro real,
    w2sigmpro real,
    w2snr real,
    w3mpro real,
    w3sigmpro real,
    w3snr real,
    w4mpro real,
    w4sigmpro real,
    w4snr real,
    nb smallint,
    na smallint,
    cc_flags character(4),
    ext_flg smallint,
    var_flg character(4),
    ph_qual character(4),
    w1mjdmean double precision,
    w2mjdmean double precision,
    w3mjdmean double precision,
    w4mjdmean double precision,
    w1gmag real,
    w1gerr real,
    w2gmag real,
    w2gerr real,
    w3gmag real,
    w3gerr real,
    w4gmag real,
    w4gerr real,
    PRIMARY KEY (designation)
);