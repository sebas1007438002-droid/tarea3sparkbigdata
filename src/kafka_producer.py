#!/usr/bin/env python3
"""
Producer Kafka - Tarea 3 (Anexo 3 mejorado)
Genera datos simulados de sensores de calidad del aire (PM10/PM2.5 + temp/humidity)
Compatible con VM vboxuser/bigdata y con local sin Kafka (modo dry-run).

Uso:
  pip install kafka-python
  python3 src/kafka_producer.py              # produce infinito cada 1s
  python3 src/kafka_producer.py --once       # solo 5 mensajes para prueba
  python3 src/kafka_producer.py --dry-run    # sin Kafka, solo imprime
"""
import time
import json
import random
import argparse
import sys

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("[WARN] kafka-python no instalado. Usando modo dry-run. Instalar con: pip install kafka-python")

def generate_sensor_data():
    # Simula sensor de calidad aire + temp/humedad (coherente con dataset batch)
    # Departamentos con mayor contaminación generan valores más altos ocasionalmente
    departamentos = ["Bogotá D.C.", "Antioquia", "Valle del Cauca", "Atlántico", "Santander"]
    dep = random.choice(departamentos)
    base_pm10 = random.gauss(35, 12)
    if dep in ["Bogotá D.C.", "Valle del Cauca"]:
        base_pm10 += random.uniform(5, 20)
    pm10 = max(5, round(base_pm10, 2))
    pm25 = max(3, round(pm10 * random.uniform(0.45, 0.65), 2))
    return {
        "sensor_id": random.randint(1, 10),
        "departamento": dep,
        "pm10": pm10,
        "pm25": pm25,
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time())  # epoch seconds
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Envía 5 mensajes y termina")
    parser.add_argument("--dry-run", action="store_true", help="No conecta a Kafka")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap")
    parser.add_argument("--topic", default="sensor_data", help="Topic Kafka")
    parser.add_argument("--total", type=int, default=0, help="Total mensajes (0=infinito, ej: --total 500)")
    parser.add_argument("--interval", type=float, default=0.05, help="Segundos entre mensajes (default 0.05, antes 1.0 lento)")
    parser.add_argument("--quiet", action="store_true", help="No imprime 1x1, solo resumen cada 100")
    parser.add_argument("--verbose", action="store_true", help="Imprime cada mensaje 1x1 (lento)")
    args = parser.parse_args()

    dry_run = args.dry_run or not KAFKA_AVAILABLE

    producer = None
    if not dry_run:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[args.bootstrap],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=3,
                # Fail-fast en vez de colgarse 60s si el broker es inalcanzable:
                max_block_ms=10000,
                request_timeout_ms=10000,
                metadata_max_age_ms=5000,
                # Fija api_version para no hacer probe extra de versión (acelera y evita 1er timeout)
                api_version=(2, 5, 0),
            )
            # Fuerza fetch de metadata AHORA (el __init__ es lazy y no conecta de verdad).
            # Si falla aquí, es problema de red/advertised.listeners, no del send().
            # partitions_for bloquea hasta max_block_ms (10s) si no hay metadata.
            partitions = producer.partitions_for(args.topic)
            if partitions is None:
                print(f"[WARN] Topic '{args.topic}' sin metadata (¿no existe?). "
                      f"Créalo con: docker exec <kafka> kafka-topics --create "
                      f"--bootstrap-server kafka:9092 --topic {args.topic} "
                      f"--partitions 1 --replication-factor 1")
            else:
                print(f"[INFO] Conectado a Kafka {args.bootstrap}, topic={args.topic}, "
                      f"partitions={partitions}")
        except Exception as e:
            print(f"[ERROR] No se pudo conectar a Kafka ({args.bootstrap}): {type(e).__name__}: {e}")
            print("[DIAG] Causas típicas de 'Failed to update metadata':")
            print("  1. El broker responde advertised.listeners=kafka:9092 pero 'kafka' no resuelve en el host.")
            print("     Fix rápido: echo '127.0.0.1 kafka' | sudo tee -a /etc/hosts")
            print("  2. Topic no existe. Verificar con: docker exec digidoc-kafka kafka-topics "
                  "--bootstrap-server kafka:9092 --list")
            print("[INFO] Cambiando a dry-run")
            dry_run = True

    count = 0
    ok = 0
    verbose = args.verbose or (args.once and not args.quiet)  # --once muestra 1x1, resto resumido
    try:
        while True:
            data = generate_sensor_data()
            if dry_run:
                if verbose:
                    print(f"[DRY-RUN] {data}")
            else:
                try:
                    future = producer.send(args.topic, value=data)
                    future.get(timeout=10)  # confirma ack o lanza excepción rápido
                    if verbose:
                        print(f"Sent: {data}")
                    ok += 1
                except Exception as e:
                    print(f"[ERROR] send() falló: {type(e).__name__}: {e}")
                    print("[DIAG] Si es KafkaTimeoutError/Failed to update metadata -> "
                          "revisa advertised.listeners y que 'kafka' resuelva a 127.0.0.1. "
                          "Ver 'python3 src/kafka_producer.py --help' y README.")
                    raise SystemExit(1)
            count += 1
            # Resumen cada 100 (rápido, no 1x1)
            if not verbose and count % 100 == 0:
                print(f"[RESUMEN] {count} msgs enviados a '{args.topic}' (ok={ok if not dry_run else count}) ...")
            if args.once and count >= 5:
                print(f"[INFO] Modo --once: enviados {count} mensajes. Fin.")
                break
            if args.total and count >= args.total:
                print(f"[RESUMEN FINAL] total={count} ok={ok if not dry_run else count} topic={args.topic}")
                break
            if args.interval > 0:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[INFO] Productor detenido por usuario (Ctrl+C)")
    finally:
        if producer:
            producer.flush()
            producer.close()

if __name__ == "__main__":
    main()
