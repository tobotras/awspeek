DROP TABLE ASSETS CASCADE;
DROP TABLE DATA_CLASSES CASCADE;
DROP TABLE REGEXPS CASCADE;
DROP TABLE MATCHES;

CREATE TABLE ASSETS (
       ID    SERIAL UNIQUE,
       NAME  VARCHAR(100) NOT NULL
);

CREATE TABLE DATA_CLASSES (
       ID    SERIAL UNIQUE,
       NAME  VARCHAR(100)
);

CREATE TABLE REGEXPS (
       ID    SERIAL UNIQUE,
       LABEL VARCHAR(100),
       REGEX VARCHAR(200),
       CLASS INT,
       
       CONSTRAINT fk_regex_class
             FOREIGN KEY(CLASS)
             REFERENCES DATA_CLASSES(ID),

       UNIQUE(CLASS, LABEL)
);

CREATE TABLE MATCHES (
       ASSET       INT,
       RESOURCE    VARCHAR(100),
       LOCATION    VARCHAR(100),
       FOLDER      VARCHAR(100),
       FILE        VARCHAR(100),
       REGEXP      INT,
       
       CONSTRAINT fk_match_asset
             FOREIGN KEY(ASSET)
             REFERENCES ASSETS(ID),
             
       CONSTRAINT fk_match_regexp
             FOREIGN KEY(REGEXP)
             REFERENCES REGEXPS(ID)
);

/* ----------------------------------- */

INSERT INTO ASSETS (NAME) VALUES
       ('AWS'),
       ('Onprem'),
       ('Filesystem')
;

INSERT INTO DATA_CLASSES (NAME) VALUES
       ( 'Healthcare' ),
       ( 'US Personal IDs' )
;

WITH data(class, label, regex) AS ( VALUES 
  ('Healthcare',        'Person name',     '^[a-zA-Z]+((['',. -][a-zA-Z ])?[a-zA-Z])$'),
  ('Healthcare',        'Person address',  '^(.\\s+)(?:#(.))?$'),
  ('US Personal IDs',   'SSN',             '^[0-9]{3}-[0-9]{2}-[0-9]{4}$'),
  ('US Personal IDs',   'Phone number',    '^\([0-9]{3}\) [0-9]{3}-[0-9]{4}$')
) INSERT INTO REGEXPS (CLASS, LABEL, REGEX)
  SELECT DATA_CLASSES.ID, data.label, data.regex
         FROM DATA_CLASSES JOIN data
         ON data.class = DATA_CLASSES.NAME;
