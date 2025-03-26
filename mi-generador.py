import sys

def generar_compose(archivo_salida, cantidad_clientes):
    cantidad = int(cantidad_clientes)
    
    compose_yaml = """name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - testing_net
    volumes:
      - ./server/config.ini:/config.ini
"""
    
    for i in range(1, cantidad + 1):
        compose_yaml += f"""  client{i}:
    container_name: client{i}
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID={i}
      - NOMBRE=Santiago Lionel
      - SURNAME=Lorca
      - DOCUMENTO=30904465
      - NACIMIENTO=1999-03-17
      - NUMERO=7574
    networks:
      - testing_net
    depends_on:
      - server
    volumes:
      - ./client/config.yaml:/config.yaml
"""

    compose_yaml += """
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""

    with open(archivo_salida, "w") as f:
        f.write(compose_yaml)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: mi-generador.py <archivo_salida> <cantidad_clientes>")
        sys.exit(1)
    
    generar_compose(sys.argv[1], sys.argv[2])