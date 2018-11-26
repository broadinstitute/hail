from .. import data

from .globals import dag_id_dag
from .job import Job
from .log import log
from .user_error import UserError


class DagNode:
    def __init__(self, spec, parents, children, dag_id):
        self.spec = spec
        self._parents = parents
        self.children = children
        self.dag_id = dag_id
        self.unfinished_parents = set([])
        self.job = None

    def name(self):
        return self.spec.name

    def fqname(self):
        return f'{self.dag_id}:{self.spec.name}'

    def add_parent(self, parent):
        self._parents.append(parent)
        self.unfinished_parents.add(parent.name())

    def add_child(self, child):
        self.children.append(child)

    def link_job(self, job):
        assert self.job is None
        self.job = job

    def to_json(self):
        return {
            'spec': self.spec.to_json(),
            'parents': [parent.name() for parent in self._parents],
            'children': [child.name() for child in self.children]
        }

    def to_get_json(self):
        return {
            'job': self.job,
            'parents': [parent.name() for parent in self._parents],
            'children': [child.name() for child in self.children]
        }

    def __str__(self):
        return str(self.to_json())

    def __repr__(self):
        return self.__str__()

    def mark_complete(self):
        exit_code = self.job.exit_code
        if exit_code == 0:
            for child in self.children:
                child.mark_parent_complete(self)
        else:
            log.warning(f'failing pod {exit_code} will not trigger dag completion')

    def mark_parent_complete(self, parent):
        parent_name = parent.name()
        log.info(f'parent {parent_name}/{parent.job.id} completed for child {self.name()}')
        if parent_name not in self.unfinished_parents:
            raise ValueError(
                f'unknown parent: {parent_name} not in {self.unfinished_parents}. '
                f'{self}')
        self.unfinished_parents.remove(parent_name)
        if not self.unfinished_parents:
            log.info(f'all parents completed for child {self.name()}')
            assert self.job is None
            self.create_job()
            assert self.job is not None

    STATUS_ORDERING = [
        'Failure', 'Cancelled', 'PendingCreation', 'Created', 'Succeeded']
    STATUS_INDEX = {v: index for index, v in enumerate(STATUS_ORDERING)}

    @staticmethod
    def status_to_int_low_is_failure(status):
        return DagNode.STATUS_INDEX[status]

    def status(self):
        return self.job.status() if self.job else 'PendingCreation'

    def create_job(self):
        return Job(self.spec.job_spec, self)

    def delete(self):
        if self.job:
            self.job.delete()

    def cancel(self):
        if self.job:
            self.job.cancel()


class Dag:
    schema = data.Dag.schema
    validator = data.Dag.validator

    @staticmethod
    def from_json(id, doc):
        return Dag(id, data.Dag.from_json(doc).nodes)

    def __init__(self, id, specs):
        self.id = id
        self.roots = []
        self.nodes = []
        self.cancelled = False

        by_name = {}
        for spec in specs:
            node = DagNode(spec, [], [], id)
            if spec.name in by_name:
                raise UserError(f'duplicate name: {spec.name}')
            by_name[spec.name] = node
            self.nodes.append(node)
        for child in by_name.values():
            if not child.spec.parent_names:
                self.roots.append(child)
            else:
                for parent_name in child.spec.parent_names:
                    parent = by_name.get(parent_name, None)
                    if not parent:
                        raise UserError(f'parent not found: {parent_name}, {by_name.keys()}')
                    parent.add_child(child)
                    child.add_parent(parent)
        for root in self.roots:
            root.create_job()

    def to_get_json(self):
        return {
            'id': self.id,
            'nodes': [node.spec.to_json() for node in self.nodes],
            'dag_cancelled': self.cancelled,
            'composite_status': self.composite_status(),
        }

    def composite_status(self):
        return min(
            [node.status() for node in self.nodes],
            key=DagNode.status_to_int_low_is_failure)

    def cancel(self):
        for node in self.nodes:
            node.cancel()
        self.cancelled = True

    def delete(self):
        for node in self.nodes:
            node.delete()
        del dag_id_dag[self.id]
