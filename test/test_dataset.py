from unittest import TestCase
from src.dataset import PandasDataset


class TestDataset(TestCase):

	def test_simple_transformation(self):
		pandas_dataset = PandasDataset("resources/test.csv", None, None).load()
		pandas_dataset.transform(lambda x: x['a'] == 4)
		pandas_dataset.compute()
		self.assertEqual(len(pandas_dataset.data.index), 2)

	def test_multi_stage_transformations(self):
		pandas_dataset = PandasDataset("resources/test.csv", None, None).load()
		pandas_dataset.transform(lambda x: x['a'] == 4)
		pandas_dataset.transform(lambda x: x['b'] > 5)
		pandas_dataset.compute()
		self.assertEqual(len(pandas_dataset.data.index), 1)

	def test_multi_stage_transformations_chained(self):
		pandas_dataset = PandasDataset("resources/test.csv", None, None).load()
		pandas_dataset\
			.transform(lambda x: x['a'] == 4)\
			.transform(lambda x: x['b'] > 5)
		pandas_dataset.compute()
		self.assertEqual(len(pandas_dataset.data.index), 1)
