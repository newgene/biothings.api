import elasticsearch

from biothings.utils.es_combat import SnapshotCompat


class Repository:
    def __init__(self, client, repository):
        # Corresponds to HTTP operations on
        # /_snapshot/<repository>

        self.client = client
        self.name = repository

    def exists(self):
        try:
            self.client.snapshot.get_repository(**SnapshotCompat.get_repository_args(self.name))
        except elasticsearch.exceptions.NotFoundError:
            return False
        return True

    def create(self, **body):
        # https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-s3-client.html
        name = body.pop("name", None) or self.name
        return self.client.snapshot.create_repository(
            **SnapshotCompat.create_repository_args(name, **body)
        )

    def delete(self):
        self.client.snapshot.delete_repository(**SnapshotCompat.delete_repository_args(self.name))

    def __str__(self):
        return (
            f"<Repository {'READY' if self.exists() else 'MISSING'}"
            f" name='{self.name}'"
            f" client={self.client}"
            f">"
        )


def test_01():
    from elasticsearch import Elasticsearch

    client = Elasticsearch()
    snapshot = Repository(client, "mynews")
    print(snapshot)


def test_02():
    from elasticsearch import Elasticsearch

    client = Elasticsearch()
    snapshot = Repository(client, "______")
    print(snapshot)


if __name__ == "__main__":
    test_01()
