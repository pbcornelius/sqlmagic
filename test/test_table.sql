@LOOP 5000000 INSERT INTO test VALUES (ROUND(RAND() * 1000000 - 500000, 0), CASEWHEN(RAND() > .8, NULL, ROUND(RAND() * 1000000 - 500000, 0)));