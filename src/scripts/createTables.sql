create table driver (
	id BIGINT primary key, 
	date_created TIMESTAMP, 
	name CHARACTER VARYING(255)
	);
	
create table passenger (
	id BIGINT primary key, 
	date_created TIMESTAMP, 
	name CHARACTER VARYING(255)
	);
	
create table booking ( 
	id BIGINT primary key, 
	date_created TIMESTAMP, 
	id_driver BIGINT not null, 
	id_passenger BIGINT not null, 
	rating INT, 
	start_date TIMESTAMP, 
	end_date TIMESTAMP,  
	tour_value BIGINT not null,
	foreign key (id_driver) REFERENCES driver(id) ON DELETE NO ACTION,
	foreign key (id_passenger) REFERENCES passenger(id) ON DELETE NO ACTION
	);