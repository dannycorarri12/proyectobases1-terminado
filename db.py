import logging
from neo4j import GraphDatabase
import csv
import io

class Database:
    def __init__(self, uri, user, password, db_name):
        """
        Inicializa la conexión con la base de datos Neo4j.
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.db_name = db_name
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            self.driver.verify_connectivity()
        except Exception as e:
            logging.error(f"Error al conectar con Neo4j: {e}")
            raise

    def close(self):
        if self.driver is not None:
            self.driver.close()

    def _get_session(self):
        """
        Método de ayuda para obtener una sesión para la base de datos correcta.
        """
        return self.driver.session(database=self.db_name)

    def _execute_query(self, query, parameters=None):
        """Función de ayuda para ejecutar consultas de LECTURA."""
        with self._get_session() as session:
            result = session.execute_read(lambda tx: tx.run(query, parameters).data())
            return result

    def _execute_write(self, tx, query, parameters=None):
        """Función de ayuda para ser usada DENTRO de una transacción de escritura."""
        tx.run(query, parameters)

    def _execute_write_fetch(self, tx, query, parameters=None):
        """Ejecuta una escritura y devuelve los resultados (lista de registros)."""
        return tx.run(query, parameters).data()

    def get_all_nodes(self, entity_label):
        """
        Obtiene todos los nodos de una etiqueta específica de forma explícita y segura.
        """
        allowed_labels = ['persona', 'libro', 'autor', 'club']
        if entity_label.lower() not in allowed_labels:
            return []
        capitalized_label = entity_label.capitalize()
        entity_details = {
            'Persona': {'props': ['id', 'nombreCompleto', 'tipoLector'], 'order': 'id'},
            'Libro': {'props': ['id', 'titulo', 'generoLiterario', 'añoPublicacion'], 'order': 'id'},
            'Autor': {'props': ['id', 'nombreCompleto', 'nacionalidad'], 'order': 'id'},
            'Club': {'props': ['id', 'nombre', 'ubicacion', 'tematica'], 'order': 'id'}
        }
        details = entity_details[capitalized_label]
        props_to_return = ', '.join([f'n.{prop} AS {prop}' for prop in details['props']])
        order_prop = details['order']
        query = f"MATCH (n:{capitalized_label}) RETURN {props_to_return} ORDER BY n.{order_prop}"
        return self._execute_query(query)

    def add_node(self, entity_label, properties):
        allowed_labels = ['persona', 'libro', 'autor', 'club']
        if entity_label.lower() not in allowed_labels:
            raise ValueError("Etiqueta no válida.")
        capitalized_label = entity_label.capitalize()
        props = dict(properties)
        if 'id' not in props or props['id'] in (None, ""):
            try:
                result = self._execute_query(
                    f"MATCH (n:{capitalized_label}) RETURN coalesce(max(n.id), 0) + 1 AS nextId"
                )
                next_id = result[0]['nextId'] if result else 1
            except Exception:
                next_id = 1
            props['id'] = int(next_id)
        query = f"CREATE (n:{capitalized_label}) SET n += $props"
        with self._get_session() as session:
            session.execute_write(self._execute_write, query, {"props": props})
    
    def get_identifier_property(self, entity_label):
        return "id"

    def update_node(self, entity_label, identifier, properties):
        allowed_labels = ['persona', 'libro', 'autor', 'club']
        if entity_label.lower() not in allowed_labels:
            raise ValueError("Etiqueta no válida.")
        capitalized_label = entity_label.capitalize()
        properties.pop('id', None)
        set_clauses = ', '.join([f"n.{key} = ${key}" for key in properties.keys()])
        if not set_clauses:
            return
        query = (
            f"MATCH (n:{capitalized_label}) "
            f"WHERE toString(n.id) = toString($identifier) "
            f"SET {set_clauses}"
        )
        parameters = {"identifier": identifier, **properties}
        with self._get_session() as session:
            session.execute_write(self._execute_write, query, parameters)
    
    def crear_relaciones(self, tipo_relacion, from_node_id, to_node_ids):
        relaciones_map = {
            'autoria': {'from_label': 'Autor', 'to_label': 'Libro', 'rel_type': 'ESCRIBIO', 'from_prop': 'nombreCompleto', 'to_prop': 'titulo'},
            'membresia': {'from_label': 'Persona', 'to_label': 'Club', 'rel_type': 'PERTENECE_A', 'from_prop': 'nombreCompleto', 'to_prop': 'nombre'},
            'lectura': {'from_label': 'Persona', 'to_label': 'Libro', 'rel_type': 'LEE', 'from_prop': 'nombreCompleto', 'to_prop': 'titulo'},
            'recomendacion': {'from_label': 'Club', 'to_label': 'Libro', 'rel_type': 'RECOMIENDA', 'from_prop': 'nombre', 'to_prop': 'titulo'}
        }
        if tipo_relacion not in relaciones_map: raise ValueError("Tipo de relación no válido.")
        config = relaciones_map[tipo_relacion]
        query = f"""
        MATCH (a:{config['from_label']})
        WHERE toString(a.id) = toString($from_id) OR a.{config['from_prop']} = $from_id
        UNWIND $to_ids AS to_id
        MATCH (b:{config['to_label']})
        WHERE toString(b.id) = toString(to_id) OR b.{config['to_prop']} = to_id
        MERGE (a)-[:{config['rel_type']}]->(b)
        """
        parameters = {"from_id": from_node_id, "to_ids": to_node_ids}
        with self._get_session() as session:
            session.execute_write(self._execute_write, query, parameters)

    def cargar_datos_iniciales(self):
        with self._get_session() as session:
            session.execute_write(self._execute_write, "MATCH (n) DETACH DELETE n")
            logging.info("Carga Automática: Base de datos anterior eliminada.")
            self._crear_esquema(session)
            queries_load = [
                "LOAD CSV WITH HEADERS FROM 'file:///Persona.csv' AS row FIELDTERMINATOR ';' CREATE (p:Persona {id: toInteger(row.id), nombreCompleto: row.Nombre, tipoLector: row.TipoLector})",
                "LOAD CSV WITH HEADERS FROM 'file:///Autor.csv' AS row FIELDTERMINATOR ';' CREATE (a:Autor {id: toInteger(row.idAutor), nombreCompleto: row.Nombre, nacionalidad: row.Nacionalidad})",
                "LOAD CSV WITH HEADERS FROM 'file:///Libro.csv' AS row FIELDTERMINATOR ';' CREATE (l:Libro {id: toInteger(row.IdLibro), titulo: row.Titulo, generoLiterario: row.Genero, añoPublicacion: toInteger(row.Anno)})",
                "LOAD CSV WITH HEADERS FROM 'file:///Club.csv' AS row FIELDTERMINATOR ';' CREATE (c:Club {id: toInteger(row.IdClub), nombre: row.Nombre, ubicacion: row.Ubicacion, tematica: row.Tematica})",
                "LOAD CSV WITH HEADERS FROM 'file:///Autor-libro.csv' AS row FIELDTERMINATOR ';' MATCH (a:Autor {id: toInteger(row.idAutor)}) MATCH (l:Libro {id: toInteger(row.idLibro)}) MERGE (a)-[:ESCRIBIO]->(l)",
                "LOAD CSV WITH HEADERS FROM 'file:///Persona-libro.csv' AS row FIELDTERMINATOR ';' MATCH (p:Persona {id: toInteger(row.id)}) MATCH (l:Libro {id: toInteger(row.idLibro)}) MERGE (p)-[:LEE]->(l)",
                "LOAD CSV WITH HEADERS FROM 'file:///Club-libro.csv' AS row FIELDTERMINATOR ';' MATCH (c:Club {id: toInteger(row.idClub)}) MATCH (l:Libro {id: toInteger(row.idLibro)}) MERGE (c)-[:RECOMIENDA]->(l)",
                "LOAD CSV WITH HEADERS FROM 'file:///Persona-club2.csv' AS row FIELDTERMINATOR ';' MATCH (p:Persona {id: toInteger(row.idPersona)}) MATCH (c:Club {id: toInteger(row.idClub)}) MERGE (p)-[:PERTENECE_A]->(c)"
            ]
            for query in queries_load:
                session.execute_write(self._execute_write, query)
            logging.info("Carga Automática: Todos los datos han sido cargados exitosamente.")
        return "Carga Automática: Todos los datos han sido cargados exitosamente en Neo4j."

    def _crear_esquema(self, session):
        queries_indices = [
            "CREATE CONSTRAINT persona_nombre IF NOT EXISTS FOR (p:Persona) REQUIRE p.nombreCompleto IS UNIQUE",
            "CREATE CONSTRAINT libro_titulo IF NOT EXISTS FOR (l:Libro) REQUIRE l.titulo IS UNIQUE",
            "CREATE CONSTRAINT autor_nombre IF NOT EXISTS FOR (a:Autor) REQUIRE a.nombreCompleto IS UNIQUE",
            "CREATE INDEX persona_id IF NOT EXISTS FOR (p:Persona) ON (p.id)",
            "CREATE INDEX libro_id IF NOT EXISTS FOR (l:Libro) ON (l.id)",
            "CREATE INDEX autor_id IF NOT EXISTS FOR (a:Autor) ON (a.id)",
            "CREATE INDEX club_id IF NOT EXISTS FOR (c:Club) ON (c.id)"
        ]
        for query in queries_indices:
            session.execute_write(self._execute_write, query)
        logging.info("Esquema (restricciones e índices) creado/verificado.")

    def _determine_delimiter(self, content):
        """Detecta el delimitador de un contenido CSV basándose en la primera línea."""
        first_line = content.splitlines()[0] if content else ""
        if "\t" in first_line:
            return "\t"
        if ";" in first_line:
            return ";"
        if "," in first_line:
            return ","
        return "\t"

    def cargar_datos_manualmente(self, file_contents):
        """Carga datos desde cualquier archivo CSV subido, detectando tipo por encabezados."""
        with self._get_session() as session:
            self._crear_esquema(session)

            def norm(s):
                return ''.join((s or '').replace('\ufeff', '').strip().lower().split())

            def safe_int(value):
                try:
                    return int(str(value).strip())
                except Exception:
                    return None

            created_summary = {
                'Persona': 0,
                'Autor': 0,
                'Libro': 0,
                'Club': 0,
                'Relaciones': 0
            }

            for filename, content in file_contents.items():
                if not content:
                    continue
                delimiter = self._determine_delimiter(content)
                reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
                if not reader.fieldnames:
                    logging.warning(f"Carga Manual: {filename} sin encabezados, omitido.")
                    continue

                header_map = {norm(h): h for h in reader.fieldnames}
                headers = set(header_map.keys())
                logging.info(f"Carga Manual: Encabezados detectados en {filename}: {list(header_map.values())}")

                if {'id', 'nombre', 'tipolector'} <= headers:
                    label = 'Persona'
                    for row in reader:
                        try:
                            raw_id = row[header_map['id']]
                            node_id = safe_int(raw_id)
                            props = {
                                'id': node_id,
                                'nombreCompleto': row[header_map['nombre']],
                                'tipoLector': row[header_map['tipolector']]
                            }
                            if node_id is None:
                                logging.warning(f"Carga Manual: Persona con id no numérico '{raw_id}' en {filename}. Omitida.")
                                continue
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} ({label}): {e}")
                            continue
                        query = f"MERGE (n:{label} {{id: $props.id}}) SET n += $props"
                        session.execute_write(self._execute_write, query, {'props': props})
                        created_summary[label] += 1
                    logging.info(f"Carga Manual: Nodos '{label}' procesados desde {filename}: {created_summary[label]}")
                    continue

                if {'idautor', 'nombre', 'nacionalidad'} <= headers:
                    label = 'Autor'
                    for row in reader:
                        try:
                            raw_id = row[header_map['idautor']]
                            node_id = safe_int(raw_id)
                            props = {
                                'id': node_id,
                                'nombreCompleto': row[header_map['nombre']],
                                'nacionalidad': row[header_map['nacionalidad']]
                            }
                            if node_id is None:
                                logging.warning(f"Carga Manual: Autor con idAutor no numérico '{raw_id}' en {filename}. Omitido.")
                                continue
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} ({label}): {e}")
                            continue
                        query = f"MERGE (n:{label} {{id: $props.id}}) SET n += $props"
                        session.execute_write(self._execute_write, query, {'props': props})
                        created_summary[label] += 1
                    logging.info(f"Carga Manual: Nodos '{label}' procesados desde {filename}: {created_summary[label]}")
                    continue

                if {'idlibro', 'titulo', 'genero', 'anno'} <= headers:
                    label = 'Libro'
                    for row in reader:
                        try:
                            raw_id = row[header_map['idlibro']]
                            node_id = safe_int(raw_id)
                            raw_anno = row[header_map['anno']]
                            anno_int = safe_int(raw_anno)
                            props = {
                                'id': node_id,
                                'titulo': row[header_map['titulo']],
                                'generoLiterario': row[header_map['genero']],
                            }
                            if anno_int is not None:
                                props['añoPublicacion'] = anno_int
                            else:
                                logging.warning(f"Carga Manual: Libro '{props['titulo']}' con anno no numérico '{raw_anno}' en {filename}. Se crea sin añoPublicacion.")
                            if node_id is None:
                                logging.warning(f"Carga Manual: Libro '{props['titulo']}' con IdLibro no numérico '{raw_id}' en {filename}. Omitido.")
                                continue
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} ({label}): {e}")
                            continue
                        query = f"MERGE (n:{label} {{id: $props.id}}) SET n += $props"
                        session.execute_write(self._execute_write, query, {'props': props})
                        created_summary[label] += 1
                    logging.info(f"Carga Manual: Nodos '{label}' procesados desde {filename}: {created_summary[label]}")
                    continue

                if {'idclub', 'nombre', 'ubicacion', 'tematica'} <= headers:
                    label = 'Club'
                    for row in reader:
                        try:
                            raw_id = row[header_map['idclub']]
                            node_id = safe_int(raw_id)
                            props = {
                                'id': node_id,
                                'nombre': row[header_map['nombre']],
                                'ubicacion': row[header_map['ubicacion']],
                                'tematica': row[header_map['tematica']]
                            }
                            if node_id is None:
                                logging.warning(f"Carga Manual: Club '{props['nombre']}' con IdClub no numérico '{raw_id}' en {filename}. Omitido.")
                                continue
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} ({label}): {e}")
                            continue
                        query = f"MERGE (n:{label} {{id: $props.id}}) SET n += $props"
                        session.execute_write(self._execute_write, query, {'props': props})
                        created_summary[label] += 1
                    logging.info(f"Carga Manual: Nodos '{label}' procesados desde {filename}: {created_summary[label]}")
                    continue

                def find_header(possible_names):
                    for name in possible_names:
                        if name in headers:
                            return header_map[name]
                    return None

                autor_col = find_header(['idautor', 'autor_id', 'autorid'])
                libro_col = find_header(['idlibro', 'libro_id', 'libroid'])
                
                if autor_col and libro_col:
                    logging.info(f"Carga Manual: Procesando relaciones Autor->Libro desde {filename}")
                    relaciones_creadas = 0
                    for row in reader:
                        try:
                            params = {
                                'from_id': int(row[autor_col]),
                                'to_id': int(row[libro_col])
                            }
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} (Autor->Libro): {e}")
                            continue
                        query = (
                            "MATCH (a:Autor) WHERE toString(a.id) = toString($from_id) "
                            "MATCH (b:Libro) WHERE toString(b.id) = toString($to_id) "
                            "MERGE (a)-[:ESCRIBIO]->(b) "
                            "RETURN 1 AS ok"
                        )
                        result = session.execute_write(self._execute_write_fetch, query, params)
                        if result:
                            created_summary['Relaciones'] += 1
                            relaciones_creadas += 1
                        else:
                            logging.warning(f"Carga Manual: No se encontró Autor(id={params['from_id']}) o Libro(id={params['to_id']}) para relación en {filename}")
                    logging.info(f"Carga Manual: {relaciones_creadas} relaciones 'ESCRIBIO' creadas desde {filename}")
                    continue

                persona_col = find_header(['id', 'persona_id', 'personaid', 'idpersona'])
                libro_col = find_header(['idlibro', 'libro_id', 'libroid'])
                
                if persona_col and libro_col:
                    logging.info(f"Carga Manual: Procesando relaciones Persona->Libro desde {filename}")
                    relaciones_creadas = 0
                    for row in reader:
                        try:
                            params = {
                                'from_id': int(row[persona_col]),
                                'to_id': int(row[libro_col])
                            }
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} (Persona->Libro): {e}")
                            continue
                        query = (
                            "MATCH (a:Persona) WHERE toString(a.id) = toString($from_id) "
                            "MATCH (b:Libro) WHERE toString(b.id) = toString($to_id) "
                            "MERGE (a)-[:LEE]->(b) "
                            "RETURN 1 AS ok"
                        )
                        result = session.execute_write(self._execute_write_fetch, query, params)
                        if result:
                            created_summary['Relaciones'] += 1
                            relaciones_creadas += 1
                        else:
                            logging.warning(f"Carga Manual: No se encontró Persona(id={params['from_id']}) o Libro(id={params['to_id']}) para relación en {filename}")
                    logging.info(f"Carga Manual: {relaciones_creadas} relaciones 'LEE' creadas desde {filename}")
                    continue

                club_col = find_header(['idclub', 'club_id', 'clubid'])
                libro_col = find_header(['idlibro', 'libro_id', 'libroid'])
                
                if club_col and libro_col:
                    logging.info(f"Carga Manual: Procesando relaciones Club->Libro desde {filename}")
                    relaciones_creadas = 0
                    for row in reader:
                        try:
                            params = {
                                'from_id': int(row[club_col]),
                                'to_id': int(row[libro_col])
                            }
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} (Club->Libro): {e}")
                            continue
                        query = (
                            "MATCH (a:Club) WHERE toString(a.id) = toString($from_id) "
                            "MATCH (b:Libro) WHERE toString(b.id) = toString($to_id) "
                            "MERGE (a)-[:RECOMIENDA]->(b) "
                            "RETURN 1 AS ok"
                        )
                        result = session.execute_write(self._execute_write_fetch, query, params)
                        if result:
                            created_summary['Relaciones'] += 1
                            relaciones_creadas += 1
                        else:
                            logging.warning(f"Carga Manual: No se encontró Club(id={params['from_id']}) o Libro(id={params['to_id']}) para relación en {filename}")
                    logging.info(f"Carga Manual: {relaciones_creadas} relaciones 'RECOMIENDA' creadas desde {filename}")
                    continue

                persona_col = find_header(['idpersona', 'persona_id', 'personaid', 'id'])
                club_col = find_header(['idclub', 'club_id', 'clubid'])
                
                if persona_col and club_col:
                    logging.info(f"Carga Manual: Procesando relaciones Persona->Club desde {filename}")
                    relaciones_creadas = 0
                    for row in reader:
                        try:
                            params = {
                                'from_id': int(row[persona_col]),
                                'to_id': int(row[club_col])
                            }
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} (Persona->Club): {e}")
                            continue
                        query = (
                            "MATCH (a:Persona) WHERE toString(a.id) = toString($from_id) "
                            "MATCH (b:Club) WHERE toString(b.id) = toString($to_id) "
                            "MERGE (a)-[:PERTENECE_A]->(b) "
                            "RETURN 1 AS ok"
                        )
                        result = session.execute_write(self._execute_write_fetch, query, params)
                        if result:
                            created_summary['Relaciones'] += 1
                            relaciones_creadas += 1
                        else:
                            logging.warning(f"Carga Manual: No se encontró Persona(id={params['from_id']}) o Club(id={params['to_id']}) para relación en {filename}")
                    logging.info(f"Carga Manual: {relaciones_creadas} relaciones 'PERTENECE_A' creadas desde {filename}")
                    continue

                logging.warning(f"Carga Manual: Formato no reconocido en {filename}, omitido.")

        resumen = (
            f"Carga Manual: OK. Personas={created_summary['Persona']}, Autores={created_summary['Autor']}, "
            f"Libros={created_summary['Libro']}, Clubes={created_summary['Club']}, Relaciones={created_summary['Relaciones']}"
        )
        return resumen

    def consulta_libros_leidos(self, persona_nombre):
        query = "MATCH (p:Persona {nombreCompleto: $nombre})-[r:LEE]->(l:Libro) RETURN l.titulo AS titulo, l.generoLiterario AS genero"
        records = self._execute_query(query, {"nombre": persona_nombre})
        return [{"titulo": record['titulo'], "genero": record['genero']} for record in records]

    def consulta_personas_club(self, club_nombre):
        query = "MATCH (p:Persona)-[:PERTENECE_A]->(c:Club {nombre: $nombre}) RETURN p.nombreCompleto AS nombre"
        return self._execute_query(query, {"nombre": club_nombre})

    def consulta_personas_mas_libros(self):
        query = """
        MATCH (p:Persona)-[:LEE]->(l:Libro)<-[:RECOMIENDA]-(c:Club)
        WITH p, c, count(l) AS librosRecomendadosLeidos
        WHERE librosRecomendadosLeidos >= 3
        RETURN p.nombreCompleto AS persona, c.nombre AS club
        """
        return self._execute_query(query)

    def consulta_personas_mas_clubes(self):
        query = """
        MATCH (p:Persona)-[:PERTENECE_A]->(c:Club)
        WITH p, count(c) AS numeroClubes
        WHERE numeroClubes > 1
        MATCH (p)-[:PERTENECE_A]->(club:Club)
        RETURN p.nombreCompleto AS persona, collect(club.nombre) AS clubes
        """
        return self._execute_query(query)

    def consulta_libros_populares(self):
        query = """
        MATCH (p:Persona)-[:LEE]->(l:Libro)
        RETURN l.titulo AS titulo, count(p) AS lectores
        ORDER BY lectores DESC
        LIMIT 3
        """
        return self._execute_query(query)
