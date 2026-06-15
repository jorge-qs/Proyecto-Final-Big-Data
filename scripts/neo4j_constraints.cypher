// Ejecutar en Neo4j Browser o con cypher-shell

CREATE CONSTRAINT user_id IF NOT EXISTS
  FOR (u:User) REQUIRE u.user_id IS UNIQUE;

CREATE CONSTRAINT biz_id IF NOT EXISTS
  FOR (b:Business) REQUIRE b.business_id IS UNIQUE;

CREATE CONSTRAINT cat_name IF NOT EXISTS
  FOR (c:Category) REQUIRE c.name IS UNIQUE;

CREATE INDEX city_name IF NOT EXISTS FOR (c:City) ON (c.name);

// Índices de apoyo para búsquedas frecuentes
CREATE INDEX user_name IF NOT EXISTS FOR (u:User) ON (u.name);
CREATE INDEX biz_city  IF NOT EXISTS FOR (b:Business) ON (b.city);
CREATE INDEX biz_stars IF NOT EXISTS FOR (b:Business) ON (b.stars);
