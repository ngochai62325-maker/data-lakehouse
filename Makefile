up:
	docker-compose up -d

down:
	docker-compose down

test:
	pytest tests/

build:
	docker-compose build
