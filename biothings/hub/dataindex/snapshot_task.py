from biothings.hub.dataindex.snapshot_repo import Repository
from biothings.utils.es_combat import SnapshotCompat


class Snapshot:
    def __init__(self, client, repository, snapshot):
        # Corresponds to HTTP operations on
        # /_snapshot/<repository>/<snapshot>

        self.client = client
        self.repository = Repository(client, repository)
        self.name = snapshot

    def exists(self):
        if self.repository.exists():
            return bool(
                self.client.snapshot.get(
                    **SnapshotCompat.get_args(
                        repo_name=self.repository.name,
                        snapshot_name=self.name,
                        ignore_unavailable=True,
                    )
                )["snapshots"]
            )
        return False

    def create(self, indices):
        self.client.snapshot.create(
            **SnapshotCompat.create_args(
                repo_name=self.repository.name,
                snapshot_name=self.name,
                indices=indices,
                include_global_state=False,
            )
        )

    def state(self):
        if self.repository.exists():
            snapshots = self.client.snapshot.get(
                **SnapshotCompat.create_args(
                    repo_name=self.repository.name,
                    snapshot_name=self.name,
                    ignore_unavailable=True,
                )
            )["snapshots"]

            if snapshots:  # [{...}]
                return snapshots[0]["state"]
            return "MISSING"

        return "N/A"

    def delete(self):
        self.client.snapshot.delete(
            **SnapshotCompat.create_args(
                repo_name=self.repository.name,
                snapshot_name=self.name,
            )
        )

    def __str__(self):
        return (
            f"<Snapshot {self.state()}"
            f" name='{self.name}'"
            f" repository={self.repository}"
            f">"
        )


def test_01():
    from elasticsearch import Elasticsearch

    client = Elasticsearch()
    snapshot = Snapshot(client, "mynews", "mynews_202012280220_vsdevjdk")
    print(snapshot)


def test_02():
    from elasticsearch import Elasticsearch

    client = Elasticsearch()
    snapshot = Snapshot(client, "mynews", "____________________________")
    print(snapshot)


if __name__ == "__main__":
    test_02()
