from unittest import TestCase
from src.dataset import PandasDataset


class TestDataset(TestCase):

	def test_simple_transformation(self):
		pandas_dataset = PandasDataset("resources/test.csv", None, None)

		pandas_dataset = pandas_dataset.transform(lambda x: x['a'] == 4)
		pandas_dataset = pandas_dataset.compute()

		assert len(pandas_dataset.data.index) == 1

