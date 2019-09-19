USE gdelt;

CREATE TABLE api1 (
    actor1_code varchar(256) NOT NULL,
    goldstein float NOT NULL,
    avg_tone float NOT NULL,
    lat decimal(10,8),
    lon decimal(11,8),
    event_date datetime NOT NULL,
    class1 bool,
    class2 tinyint,
    timing float
)ENGINE=InnoDB;
