#Creamos base de datos
CREATE DATABASE transactionsT4;
USE transactionsT4;

#Creamos tablas
CREATE TABLE companies (
	company_id VARCHAR(8) PRIMARY KEY,
    company_name VARCHAR (255),
    phone VARCHAR (50),
    email VARCHAR (50),
    country VARCHAR (50),
    website VARCHAR (50)
    );


CREATE TABLE credit_cards (
	id VARCHAR (10) PRIMARY KEY,
    user_id VARCHAR (10),
    iban VARCHAR (50),
    pan VARCHAR (50),
    pin VARCHAR (4),
    cvv VARCHAR (3),
    track1 VARCHAR (50),
    track2 VARCHAR (50),
    expiring_date VARCHAR (10)
    );

#Creamos la tabla users_all para poder unir las tres tablas de usuarios en una sola
CREATE TABLE users_all (
	id INTEGER PRIMARY KEY,
    name VARCHAR (50),
    surname VARCHAR (50),
    phone VARCHAR (50),
    email VARCHAR (50),
    birth_date VARCHAR (50),
    country VARCHAR (50),
    city VARCHAR (50),
    postal_code VARCHAR (10),
    address VARCHAR (50)
    );
    
CREATE TABLE transactions (
	id VARCHAR (50) PRIMARY KEY,
    card_id VARCHAR (10),
    business_id VARCHAR(8),
    timestamp VARCHAR (50),
    amount DECIMAL (10,2),
    declined BOOLEAN,
    products_ids VARCHAR (50),
    user_id INT,
    lat VARCHAR (50),
    longitude VARCHAR (50),
    FOREIGN KEY (card_id) REFERENCES credit_cards(id),
    FOREIGN KEY (business_id) REFERENCES companies(company_id),
    FOREIGN KEY (user_id) REFERENCES users_all(id)
    );
    
# Insertamos los datos en las tablas utilizando la función "Table Date Import Wizard"
# y corroboramos la correcta importación de datos
SELECT * FROM companies;
SELECT * FROM credit_cards;
SELECT * FROM users_all;
SELECT * FROM transactions;

#Nivell 1
##N1. Exercici 1: subconsulta que mostri tots els usuaris amb més de 30 transaccions utilitzant almenys 2 taules.
SELECT id
FROM users_all
WHERE id IN ( 
	SELECT user_id 
	FROM transactions
	GROUP BY user_id
	HAVING COUNT(transactions.id) >= 30);


##N1. Exercici 2: Mostra la mitjana de la suma de transaccions per IBAN de les targetes de crèdit en la companyia Donec Ltd. utilitzant almenys 2 taules.
WITH tt AS
	(SELECT business_id, sum(amount) AS suma_trans
	FROM transactions AS t
	JOIN credit_cards AS cc
	ON t.card_id=cc.id
	GROUP BY business_id, iban)
SELECT AVG(suma_trans)
FROM tt
WHERE business_id IN (
	SELECT company_id
	FROM companies
	WHERE company_name LIKE 'donec ltd%');
    
##otra opción con tabla temporal, pero sin subquery y solo joins
WITH tt AS(
	SELECT business_id, sum(amount) AS suma_trans
	FROM transactions AS t
	JOIN credit_cards AS cc
	ON t.card_id=cc.id
	JOIN companies AS c 
	ON c.company_id = t.business_id
	WHERE company_name LIKE 'Donec Ltd%'
	GROUP BY t.business_id, cc.iban)
SELECT AVG(suma_trans)
FROM tt;


#Nivell 2: Crea una nova taula que reflecteixi l'estat de les targetes de crèdit basat en si les últimes tres transaccions van ser declinades 
##cambiamos el nombre del campo timestamp por fecha_hora para evitar errores involutarios
SET SQL_SAFE_UPDATES = 0;
ALTER TABLE transactions CHANGE timestamp fecha_hora VARCHAR(50);
SET SQL_SAFE_UPDATES = 1;

#damos formato correcto de tipo datetime al campo fecha_hora para poder ordenar por este valor
SET SQL_SAFE_UPDATES = 0;
UPDATE transactions
SET fecha_hora = STR_TO_DATE(fecha_hora, '%d/%m/%Y %H:%i');
SET SQL_SAFE_UPDATES = 1;

DESCRIBE transactions;
ALTER TABLE transactions MODIFY fecha_hora DATETIME;
DESCRIBE transactions;

#---------------------------------------------------------------------------------
#Creamos la tabla

CREATE TABLE card_status AS (
WITH rn_table AS (
	SELECT t.card_id, t.fecha_hora, t.declined,
	ROW_NUMBER () OVER (PARTITION BY card_id ORDER BY fecha_hora DESC) AS rn
FROM transactions AS t)
SELECT *,
	CASE 
		WHEN declined = 1 AND rn < 4 AND
        (SELECT SUM(declined) FROM rn_table AS inner_table WHERE inner_table.card_id = rn_table.card_id) >= 3 
    THEN 'Rechazada'
    ELSE 'Aceptada'
END AS Status
FROM rn_table);

#Corroboramos la creación de la tabla
DESCRIBE card_status;

SELECT * FROM card_status;


#----------------------------PRUEBA PARA CORROBORAR QUE CASE FUNCIONA---------------------------
#Cambiaremos a declined los datos de una tarjeta para que cumpla las condiciones de rechazo y verificar CASE
DROP TABLE card_status;

#averiguamos el id de las últimas tres transacciones de la compañía con card_id CcU-2938 para cambiarlas a declined = 1
SELECT * FROM transactions
WHERE card_id LIKE '%2938'
ORDER BY fecha_hora DESC;

#Hacemos que las últimas tres transacciones sean declinadas
SET SQL_SAFE_UPDATES = 0;
UPDATE transactions
SET declined = 1 
WHERE id IN ('AD85A78A-8829-5746-93A0-8B7A792EBC18', 'F1A598A2-86C5-50A9-F1CE-FB1D69866C39', '55166D02-D74C-6A63-6C54-8678467649B4');

#Creamos la tabla de nuevo
#el código evaluaba correctamente, lo modificamos hasta que funciona. Creamos la tabla definitiva:
CREATE TABLE card_status AS (
WITH rn_table AS (
	SELECT t.card_id, t.fecha_hora, t.declined,
	ROW_NUMBER () OVER (PARTITION BY card_id ORDER BY fecha_hora DESC) AS rn
FROM transactions AS t)
SELECT *,
	CASE 
		WHEN declined = 1 AND rn < 4 AND
        (SELECT SUM(declined) FROM rn_table AS inner_table WHERE inner_table.card_id = rn_table.card_id) >= 3 
    THEN 'Rechazada'
    ELSE 'Aceptada'
END AS Status
FROM rn_table
);

#corroboramos:
SELECT * FROM card_status;

#volvemos a dejar los datos de la tabla transaccion como estaban originalmente, eliminamos y creamos de nuevo la tabla card_status
DROP TABLE card_status;

UPDATE transactions
SET declined = 0 
WHERE id IN ('AD85A78A-8829-5746-93A0-8B7A792EBC18', 'F1A598A2-86C5-50A9-F1CE-FB1D69866C39', '55166D02-D74C-6A63-6C54-8678467649B4');

SET SQL_SAFE_UPDATES = 1;

#------------------------------------------ACABADA LA COMPROBACIÓN DEL CÓDIGO CASE------------------

#Creamos la tabla card_status sabiendo que funciona correctamente:
CREATE TABLE card_status AS (
WITH rn_table AS (
	SELECT t.card_id, t.fecha_hora, t.declined,
	ROW_NUMBER () OVER (PARTITION BY card_id ORDER BY fecha_hora DESC) AS rn
FROM transactions AS t)
SELECT *,
	CASE 
		WHEN declined = 1 AND rn < 4 AND
        (SELECT SUM(declined) FROM rn_table AS inner_table WHERE inner_table.card_id = rn_table.card_id) >= 3 
    THEN 'Inactiva'
    ELSE 'Activa'
END AS Status
FROM rn_table);


SELECT * FROM card_status;

#---------------------------------------------------------------

##N2: Exercici 1: Quantes targetes estan actives?

SELECT COUNT(distinct card_id)
FROM card_status
WHERE status = 'Activa';

##Nivell 3
## Primero creamos la tabla Products e importamos los datos con wizard
CREATE TABLE products (
	id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(50),
    price VARCHAR(50),
    colour VARCHAR(50),
    weight VARCHAR(50),
    warehouse_id VARCHAR(50));
 
#comprobamos la creación de la tabla
SELECT * FROM products;


#Luego creamos la siguiente tabla puente para evitar la relación N-N entre las tablas Products y Transactions
CREATE TABLE products_per_transactions (
	id_transaction VARCHAR(50),
    id_product VARCHAR(50));

#Comprobamos la creación de la tabla
SELECT * FROM products_per_transactions;


#Insertamos los datos en la tabla puente a partir de campos que ya existen en la tabla Transactions
INSERT INTO products_per_transactions (id_transaction, id_product)
SELECT t.id AS id_transaction,
       SUBSTRING_INDEX(SUBSTRING_INDEX(t.products_ids, ',', numbers.n), ',', -1) AS id_product #aqui separamos cada id de producto del campo products_ids
FROM transactions t
JOIN (
    SELECT ROW_NUMBER() OVER () AS n #aqui generamos una secuencia de números para cada fila de la tabla t
    FROM transactions
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS numbers
     #aqui hacemos una cross join con una tabla de 10 filas para asegurarnos que habrá sitio para al menos 10 códigos de producto por cada transacción 
) AS numbers 
ON numbers.n <= LENGTH(t.products_ids) - LENGTH(REPLACE(t.products_ids, ',', '')) + 1; #aqui calculamos la cantidad de comas que hay en cada campo y le sumamos uno, para obtener la cantidad de productos totales por campo

#Comprobamos la inserción correcta de los datos
SELECT * FROM products_per_transactions;

#descubrimos que hay espacios en blanco en el campo, los eliminamos con TRIM
select length(id_product)
from products_per_transactions;

#eliminamos los espacios en blanco con la función TRIM
SET SQL_SAFE_UPDATES = 0;
UPDATE products_per_transactions
SET id_product = TRIM(id_product);
SET SQL_SAFE_UPDATES = 1;

#Ahora añadimos las FK
ALTER TABLE products_per_transactions
ADD CONSTRAINT FOREIGN KEY (id_transaction) REFERENCES transactions(id);

### Creamos un índice porque sino da error al querer crear la FK
CREATE INDEX idx_products_id ON products(id);
ALTER TABLE products_per_transactions
ADD CONSTRAINT FOREIGN KEY (id_product) REFERENCES products (id);

#N3: E1: Necessitem conèixer el nombre de vegades que s'ha venut cada produ
SELECT id_product, COUNT(distinct id_transaction)
FROM products_per_transactions ppt
JOIN transactions t
ON ppt.id_transaction=t.id
WHERE declined=0
GROUP BY id_product;







    

