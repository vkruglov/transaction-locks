CREATE TABLE person
(
    id         int NOT NULL,
    first_name varchar(255) DEFAULT NULL,
    last_name  varchar(255) DEFAULT NULL,
    PRIMARY KEY (id)
);

INSERT INTO person (id, first_name, last_name)
VALUES (1, 'John', 'Doe');
INSERT INTO person (id, first_name, last_name)
VALUES (2, 'Jane', 'Doe');
