-- The actual dataset has comments and SQL statements at the beginning but the extractor
-- is only concern with lines starting with "INSERT INTO `externallinks` VALUES"
-- It is assumed that there is no empty lines and leading / trailing white spaces
INSERT INTO `externallinks` VALUES (1,1,'http://1a.example.com/bleedin\'','http://1b.example.com','http://1c.example.com');
INSERT INTO `externallinks` VALUES (2,1,'http://2a.example.com','http://2b.example.com/bleedin\'','http://2c.example.com'),(3,1,'//3a.example.com','//3b.example.com','//3c.example.com/bleedin\'');