## LIST OF CHARACTERS AND THEIR LOCATIONS

SELECT c.name AS CharacterName, l.name AS LocationName
FROM Character_Table c
JOIN Location_Table l ON c.location_id = l.location_id;


## COUNT CHARACTERS BY LOCATION

SELECT l.name AS LocationName, COUNT(c.character_id) AS CharacterCount
FROM Character_Table c
JOIN Location_Table l ON c.location_id = l.location_id
GROUP BY l.name
ORDER BY CharacterCount DESC;

## LIST OF UNIQUE LOCATIONS WITH NO CHARACTERS

SELECT l.name AS LocationName
FROM Location_Table l
LEFT JOIN Character_Table c ON l.location_id = c.location_id
WHERE c.location_id IS NULL;


## TOP 5 LOCATIONS WITH MOST CHARACTERS 

SELECT l.name AS LocationName, COUNT(c.character_id) AS CharacterCount
FROM Character_Table c
JOIN Location_Table l ON c.location_id = l.location_id
GROUP BY l.name
ORDER BY CharacterCount DESC
LIMIT 5;

## COUNT OF CHARACTERS BY SPECIES

SELECT species, COUNT(character_id) AS CharacterCount
FROM Character_Table
GROUP BY species
ORDER BY CharacterCount DESC;


