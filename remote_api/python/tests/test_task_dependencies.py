import unittest
import lynx.luigi


class BaseTask(lynx.luigi.LynxTableFileTask):

  def compute_view(self):
    return self.lk.sql('select 1 as i')


class DepTask(lynx.luigi.LynxTableFileTask):

  def requires(self):
    return BaseTask()

  def compute_view(self):
    return self.lk.sql('select 2 * i as j from b', b=self.inputview())


class DictDepTask(lynx.luigi.LynxTableFileTask):

  def requires(self):
    return {'b': BaseTask()}

  def compute_view(self):
    return self.lk.sql('select 2 * i as j from b', **self.inputview())


class ListDepTask(lynx.luigi.LynxTableFileTask):

  def requires(self):
    yield BaseTask()

  def compute_view(self):
    b = self.inputview()[0]
    return self.lk.sql('select 2 * i as j from b', b=b)


class TestTaskDependencies(unittest.TestCase):

  def test_dependency_view(self):
    BaseTask().run()
    for i in [DepTask(), DictDepTask(), ListDepTask()]:
      i.run()
      self.assertEqual(i.output().view(i.lk).take(1)[0]['j'], '2')
