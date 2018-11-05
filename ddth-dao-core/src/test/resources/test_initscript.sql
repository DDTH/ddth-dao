SET GLOBAL time_zone = '+07:00';

DROP TABLE IF EXISTS $table$;
CREATE TABLE $table$ (
    id                  BIGINT          AUTO_INCREMENT,
    username            VARCHAR(32),
        UNIQUE INDEX (username),
    yob                 INT,
    fullname            VARCHAR(255),
    data_date           DATE,
    data_time           TIME(3),
    data_datetime       DATETIME(3),
    data_bin            BLOB,
    data_notnull        INT             UNSIGNED NOT NULL,
    PRIMARY KEY (id)
) Engine=InnoDB;

INSERT INTO $table$ (id, username, yob, fullname, data_date, data_time, data_datetime, data_bin, data_notnull)
VALUES
    (1, 'a', '1999', 'Nguyen A', NOW(), NOW(), NOW(), '', 1)
   ,(2, 'b', '2000', 'Nguyen B', NOW(), NOW(), NOW(), '', 2)
   ,(3, 'c', '2001', 'Nguyen C', NOW(), NOW(), NOW(), '', 3)
   ;
