DROP TABLE IF EXISTS $table$;
CREATE TABLE $table$ (
    id                  BIGSERIAL,
    username            VARCHAR(32),
    yob                 INT,
    fullname            VARCHAR(255),
    data_date           DATE,
    data_time           TIME,
    data_datetime       TIMESTAMP,
    data_bin            BYTEA,
    data_notnull        INT             NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX $table$_username ON $table$(username);

INSERT INTO $table$ (id, username, yob, fullname, data_date, data_time, data_datetime, data_bin, data_notnull)
VALUES
    (1, 'a', '1999', 'Nguyen A', NOW(), NOW(), NOW(), '', 1)
   ,(2, 'b', '2000', 'Nguyen B', NOW(), NOW(), NOW(), '', 2)
   ,(3, 'c', '2001', 'Nguyen C', NOW(), NOW(), NOW(), '', 3)
   ;
