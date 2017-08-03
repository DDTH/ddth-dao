DROP TABLE IF EXISTS tbl_user;
CREATE TABLE tbl_user (
    id                  BIGINT          AUTO_INCREMENT,
    username            VARCHAR(32),
        UNIQUE INDEX (username),
    yob                 INT,
    fullname            VARCHAR(255),
    data_date           DATE,
    data_time           TIME,
    data_datetime       DATETIME,
    data_bin            BLOB,
    PRIMARY KEY (id)
) Engine=InnoDB;

INSERT INTO tbl_user (id, username, yob, fullname, data_date, data_time, data_datetime, data_bin)
VALUES
    (1, 'a', '1999', 'Nguyen A', NOW(), NOW(), NOW(), '')
   ,(2, 'b', '2000', 'Nguyen B', NOW(), NOW(), NOW(), '')
   ,(3, 'c', '2001', 'Nguyen C', NOW(), NOW(), NOW(), '')
   ;
