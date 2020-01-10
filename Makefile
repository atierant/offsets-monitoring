test:
	./vendor/bin/phpunit --bootstrap vendor/autoload.php tests

shell:
	docker run --network host --rm -it -u `id -u`:`id -g` -w /srv -v `pwd`:/srv charlycoste/php:7.1-kafka bash
