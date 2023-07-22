CREATE TABLE PERSONS (
       ID SERIAL UNIQUE,
       FIRST_NAME VARCHAR(100),
       LAST_NAME VARCHAR(100),
       PHONE VARCHAR(50),
       EMAIL VARCHAR(100)
);

/* ----------------------------------- */

INSERT INTO PERSONS(FIRST_NAME, LAST_NAME, PHONE, EMAIL) VALUES
       ('Ivan',   'Ivanov', '(123) 322-2233', 'ivan@yandex.ru')
;
