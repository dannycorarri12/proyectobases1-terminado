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
            'Persona': {'props': ['nombreCompleto', 'tipoLector'], 'id': 'nombreCompleto'},
            'Libro': {'props': ['titulo', 'generoLiterario', 'añoPublicacion'], 'id': 'titulo'},
            'Autor': {'props': ['nombreCompleto', 'nacionalidad'], 'id': 'nombreCompleto'},
            'Club': {'props': ['nombre', 'ubicacion', 'tematica'], 'id': 'nombre'}
        }
        details = entity_details[capitalized_label]
        props_to_return = ', '.join([f'n.{prop} AS {prop}' for prop in details['props']])
        identifier = details['id']
        query = f"MATCH (n:{capitalized_label}) RETURN {props_to_return} ORDER BY n.{identifier}"
        return self._execute_query(query)

    def add_node(self, entity_label, properties):
        allowed_labels = ['persona', 'libro', 'autor', 'club']
        if entity_label.lower() not in allowed_labels: raise ValueError("Etiqueta no válida.")
        capitalized_label = entity_label.capitalize()
        set_clauses = ', '.join([f"n.{key} = ${key}" for key in properties.keys()])
        query = f"CREATE (n:{capitalized_label}) SET {set_clauses}"
        with self._get_session() as session:
            session.execute_write(self._execute_write, query, properties)
    
    def get_identifier_property(self, entity_label):
        capitalized_label = entity_label.capitalize()
        if capitalized_label == "Libro": return "titulo"
        elif capitalized_label == "Club": return "nombre"
        return "nombreCompleto"

    def update_node(self, entity_label, identifier, properties):
        allowed_labels = ['persona', 'libro', 'autor', 'club']
        if entity_label.lower() not in allowed_labels: raise ValueError("Etiqueta no válida.")
        capitalized_label = entity_label.capitalize()
        id_property = self.get_identifier_property(entity_label)
        properties.pop(id_property, None)
        set_clauses = ', '.join([f"n.{key} = ${key}" for key in properties.keys()])
        if not set_clauses: return
        query = f"MATCH (n:{capitalized_label} {{{id_property}: $identifier}}) SET {set_clauses}"
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
        WHERE toString(a.csvId) = toString($from_id) OR a.{config['from_prop']} = $from_id
        UNWIND $to_ids AS to_id
        MATCH (b:{config['to_label']})
        WHERE toString(b.csvId) = toString(to_id) OR b.{config['to_prop']} = to_id
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
                "LOAD CSV WITH HEADERS FROM 'file:///Persona.csv' AS row FIELDTERMINATOR ';' CREATE (p:Persona {nombreCompleto: row.Nombre, tipoLector: row.TipoLector, csvId: toInteger(row.id)})",
                "LOAD CSV WITH HEADERS FROM 'file:///Autor.csv' AS row FIELDTERMINATOR ';' CREATE (a:Autor {nombreCompleto: row.Nombre, nacionalidad: row.Nacionalidad, csvId: toInteger(row.idAutor)})",
                "LOAD CSV WITH HEADERS FROM 'file:///Libro.csv' AS row FIELDTERMINATOR ';' CREATE (l:Libro {titulo: row.Titulo, generoLiterario: row.Genero, añoPublicacion: toInteger(row.Anno), csvId: toInteger(row.IdLibro)})",
                "LOAD CSV WITH HEADERS FROM 'file:///Club.csv' AS row FIELDTERMINATOR ';' CREATE (c:Club {nombre: row.Nombre, ubicacion: row.Ubicacion, tematica: row.Tematica, csvId: toInteger(row.IdClub)})",
                "LOAD CSV WITH HEADERS FROM 'file:///Autor-libro.csv' AS row FIELDTERMINATOR ';' MATCH (a:Autor {csvId: toInteger(row.idAutor)}) MATCH (l:Libro {csvId: toInteger(row.idLibro)}) MERGE (a)-[:ESCRIBIO]->(l)",
                "LOAD CSV WITH HEADERS FROM 'file:///Persona-libro.csv' AS row FIELDTERMINATOR ';' MATCH (p:Persona {csvId: toInteger(row.id)}) MATCH (l:Libro {csvId: toInteger(row.idLibro)}) MERGE (p)-[:LEE]->(l)",
                "LOAD CSV WITH HEADERS FROM 'file:///Club-libro.csv' AS row FIELDTERMINATOR ';' MATCH (c:Club {csvId: toInteger(row.idClub)}) MATCH (l:Libro {csvId: toInteger(row.idLibro)}) MERGE (c)-[:RECOMIENDA]->(l)",
                "LOAD CSV WITH HEADERS FROM 'file:///Persona-club2.csv' AS row FIELDTERMINATOR ';' MATCH (p:Persona {csvId: toInteger(row.idPersona)}) MATCH (c:Club {csvId: toInteger(row.idClub)}) MERGE (p)-[:PERTENECE_A]->(c)"
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
            "CREATE INDEX persona_csv_id IF NOT EXISTS FOR (p:Persona) ON (p.csvId)",
            "CREATE INDEX libro_csv_id IF NOT EXISTS FOR (l:Libro) ON (l.csvId)",
            "CREATE INDEX autor_csv_id IF NOT EXISTS FOR (a:Autor) ON (a.csvId)",
            "CREATE INDEX club_csv_id IF NOT EXISTS FOR (c:Club) ON (c.csvId)"
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
        # Valor por defecto: tabulación, ya que los archivos del proyecto usan tabs
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

                # --- Detección y carga de nodos por encabezados ---
                if {'id', 'nombre', 'tipolector'} <= headers:
                    label = 'Persona'
                    for row in reader:
                        try:
                            raw_id = row[header_map['id']]
                            csv_id = safe_int(raw_id)
                            props = {
                                'nombreCompleto': row[header_map['nombre']],
                                'tipoLector': row[header_map['tipolector']],
                                # Guardar también el ID original para fallbacks en relaciones
                                'id': raw_id
                            }
                            if csv_id is not None:
                                props['csvId'] = csv_id
                            else:
                                logging.warning(f"Carga Manual: Persona con id no numérico '{raw_id}' en {filename}. Se crea sin csvId.")
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} ({label}): {e}")
                            continue
                        query = f"MERGE (n:{label} {{nombreCompleto: $props.nombreCompleto}}) SET n += $props"
                        session.execute_write(self._execute_write, query, {'props': props})
                        created_summary[label] += 1
                    logging.info(f"Carga Manual: Nodos '{label}' procesados desde {filename}: {created_summary[label]}")
                    continue

                if {'idautor', 'nombre', 'nacionalidad'} <= headers:
                    label = 'Autor'
                    for row in reader:
                        try:
                            raw_id = row[header_map['idautor']]
                            csv_id = safe_int(raw_id)
                            props = {
                                'nombreCompleto': row[header_map['nombre']],
                                'nacionalidad': row[header_map['nacionalidad']],
                                # Guardar también el ID original para fallbacks en relaciones
                                header_map['idautor']: raw_id
                            }
                            if csv_id is not None:
                                props['csvId'] = csv_id
                            else:
                                logging.warning(f"Carga Manual: Autor con idAutor no numérico '{raw_id}' en {filename}. Se crea sin csvId.")
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} ({label}): {e}")
                            continue
                        query = f"MERGE (n:{label} {{nombreCompleto: $props.nombreCompleto}}) SET n += $props"
                        session.execute_write(self._execute_write, query, {'props': props})
                        created_summary[label] += 1
                    logging.info(f"Carga Manual: Nodos '{label}' procesados desde {filename}: {created_summary[label]}")
                    continue

                if {'idlibro', 'titulo', 'genero', 'anno'} <= headers:
                    label = 'Libro'
                    for row in reader:
                        try:
                            raw_id = row[header_map['idlibro']]
                            csv_id = safe_int(raw_id)
                            raw_anno = row[header_map['anno']]
                            anno_int = safe_int(raw_anno)
                            props = {
                                'titulo': row[header_map['titulo']],
                                'generoLiterario': row[header_map['genero']],
                                # Guardar también el ID original para fallbacks en relaciones (IdLibro o idlibro según archivo)
                                header_map['idlibro']: raw_id
                            }
                            if anno_int is not None:
                                props['añoPublicacion'] = anno_int
                            else:
                                logging.warning(f"Carga Manual: Libro '{props['titulo']}' con anno no numérico '{raw_anno}' en {filename}. Se crea sin añoPublicacion.")
                            if csv_id is not None:
                                props['csvId'] = csv_id
                            else:
                                logging.warning(f"Carga Manual: Libro '{props['titulo']}' con IdLibro no numérico '{raw_id}' en {filename}. Se crea sin csvId.")
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} ({label}): {e}")
                            continue
                        query = f"MERGE (n:{label} {{titulo: $props.titulo}}) SET n += $props"
                        session.execute_write(self._execute_write, query, {'props': props})
                        created_summary[label] += 1
                    logging.info(f"Carga Manual: Nodos '{label}' procesados desde {filename}: {created_summary[label]}")
                    continue

                if {'idclub', 'nombre', 'ubicacion', 'tematica'} <= headers:
                    label = 'Club'
                    for row in reader:
                        try:
                            raw_id = row[header_map['idclub']]
                            csv_id = safe_int(raw_id)
                            props = {
                                'nombre': row[header_map['nombre']],
                                'ubicacion': row[header_map['ubicacion']],
                                'tematica': row[header_map['tematica']],
                                # Guardar también el ID original para fallbacks (IdClub o idclub)
                                header_map['idclub']: raw_id
                            }
                            if csv_id is not None:
                                props['csvId'] = csv_id
                            else:
                                logging.warning(f"Carga Manual: Club '{props['nombre']}' con IdClub no numérico '{raw_id}' en {filename}. Se crea sin csvId.")
                        except Exception as e:
                            logging.warning(f"Carga Manual: Fila inválida en {filename} ({label}): {e}")
                            continue
                        # Para alinear con la carga automática y evitar colapsar clubs con el mismo nombre,
                        # hacemos MERGE por csvId cuando esté disponible; de lo contrario, CREATE.
                        if 'csvId' in props:
                            query = f"MERGE (n:{label} {{csvId: $props.csvId}}) SET n += $props"
                        else:
                            query = f"CREATE (n:{label}) SET n += $props"
                        session.execute_write(self._execute_write, query, {'props': props})
                        created_summary[label] += 1
                    logging.info(f"Carga Manual: Nodos '{label}' procesados desde {filename}: {created_summary[label]}")
                    continue

                # --- Detección y carga de relaciones por encabezados ---
                # Función auxiliar para encontrar header con variaciones de capitalización
                def find_header(possible_names):
                    for name in possible_names:
                        if name in headers:
                            return header_map[name]
                    return None

                # Autor->Libro: buscar variaciones de idautor e idlibro
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
                            "MATCH (a:Autor) WHERE toString(coalesce(a.csvId, a.idAutor, a.IdAutor, a.id, a.Id)) = toString($from_id) "
                            "MATCH (b:Libro) WHERE toString(coalesce(b.csvId, b.idlibro, b.IdLibro, b.id, b.Id)) = toString($to_id) "
                            "MERGE (a)-[:ESCRIBIO]->(b) "
                            "RETURN 1 AS ok"
                        )
                        result = session.execute_write(self._execute_write_fetch, query, params)
                        if result:
                            created_summary['Relaciones'] += 1
                            relaciones_creadas += 1
                        else:
                            logging.warning(f"Carga Manual: No se encontró Autor(csvId={params['from_id']}) o Libro(csvId={params['to_id']}) para relación en {filename}")
                    logging.info(f"Carga Manual: {relaciones_creadas} relaciones 'ESCRIBIO' creadas desde {filename}")
                    continue

                # Persona->Libro: buscar variaciones de id e idlibro
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
                            "MATCH (a:Persona) WHERE toString(coalesce(a.csvId, a.idPersona, a.IdPersona, a.id, a.Id)) = toString($from_id) "
                            "MATCH (b:Libro) WHERE toString(coalesce(b.csvId, b.idlibro, b.IdLibro, b.id, b.Id)) = toString($to_id) "
                            "MERGE (a)-[:LEE]->(b) "
                            "RETURN 1 AS ok"
                        )
                        result = session.execute_write(self._execute_write_fetch, query, params)
                        if result:
                            created_summary['Relaciones'] += 1
                            relaciones_creadas += 1
                        else:
                            logging.warning(f"Carga Manual: No se encontró Persona(csvId={params['from_id']}) o Libro(csvId={params['to_id']}) para relación en {filename}")
                    logging.info(f"Carga Manual: {relaciones_creadas} relaciones 'LEE' creadas desde {filename}")
                    continue

                # Club->Libro: buscar variaciones de idclub e idlibro
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
                            "MATCH (a:Club) WHERE toString(coalesce(a.csvId, a.idClub, a.IdClub, a.id, a.Id)) = toString($from_id) "
                            "MATCH (b:Libro) WHERE toString(coalesce(b.csvId, b.idlibro, b.IdLibro, b.id, b.Id)) = toString($to_id) "
                            "MERGE (a)-[:RECOMIENDA]->(b) "
                            "RETURN 1 AS ok"
                        )
                        result = session.execute_write(self._execute_write_fetch, query, params)
                        if result:
                            created_summary['Relaciones'] += 1
                            relaciones_creadas += 1
                        else:
                            logging.warning(f"Carga Manual: No se encontró Club(csvId={params['from_id']}) o Libro(csvId={params['to_id']}) para relación en {filename}")
                    logging.info(f"Carga Manual: {relaciones_creadas} relaciones 'RECOMIENDA' creadas desde {filename}")
                    continue

                # Persona->Club: buscar variaciones de idpersona e idclub
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
                            "MATCH (a:Persona) WHERE toString(coalesce(a.csvId, a.idPersona, a.IdPersona, a.id, a.Id)) = toString($from_id) "
                            "MATCH (b:Club) WHERE toString(coalesce(b.csvId, b.idClub, b.IdClub, b.id, b.Id)) = toString($to_id) "
                            "MERGE (a)-[:PERTENECE_A]->(b) "
                            "RETURN 1 AS ok"
                        )
                        result = session.execute_write(self._execute_write_fetch, query, params)
                        if result:
                            created_summary['Relaciones'] += 1
                            relaciones_creadas += 1
                        else:
                            logging.warning(f"Carga Manual: No se encontró Persona(csvId={params['from_id']}) o Club(csvId={params['to_id']}) para relación en {filename}")
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