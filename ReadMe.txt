1) SHOW TABLES;
	show tables;
	
2) CREATE TABLE <TABLE_NAME> ( ROW_ID INT , <COLUMN_NAME 1> <DATA_TYPE>, ..., <COLUMN_NAME n> <DATA_TYPE> ) ;
	create table employee ( row_id int , employee_id int , employee_name text ) ;

3) SELECT * FROM <TABLE_NAME> WHERE <CONDITION> ;
	select * from employee ;
	select * from employee where row_id = 2 ;
	select * from employee where employee_id = 3 ;
	select * from employee where employee_name = sravya ;

4) INSERT INTO <TABLE_NAME> ( <COLUMN_NAME 1> , <COLUMN_NAME 2> , ..., <COLUMN_NAME n> )  VALUES ( <VALUES 1> , <VALUES 2>, ...., <VALUES n> ) ;
	insert into employee ( row_id , employee_id , employee_name ) values (1,1,karthik) ; // to be tested....
	insert into employee ( row_id , employee_id , employee_name ) values (2,2,sravya) ;
	
5) UPDATE <TABLE_NAME> SET <COLUMN_NAME> = <VALUE> WHERE <CONDITION> ;
	update employee set name = sravya_mam where row_id = 2 ;
	update employee set employee_name = Karthik_Reddy where employee_id = 1 ;
	update employee set employee_id = 3 where employee_name = sravya_mam ;
	
6) DELETE FROM <TABLE_NAME> WHERE <COLUMN_NAME> = <VALUE> ;
	delete from employee where row_id = 2 ;
	
7) DROP TABLE <TABLE_NAME> ;
	drop table employer ;

