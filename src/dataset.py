import pandas as pd


class Dataset(object):

	def __init__(self, input_path, output_path, schema):
		self.input_path = input_path
		self.output_path = output_path
		self.schema = schema
		self.transform = None

	def transform(self, transform):
		self.transform = transform

	def _load(self):
		raise NotImplementedError()

	def _apply(self):
		# actually apply the transformation to the dataset
		raise NotImplementedError()

	def _write(self):
		raise NotImplementedError()


class PandasDataset(Dataset):

	def __init__(self, input_path, output_path, schema):
		super(PandasDataset, self).__init__(input_path, output_path, schema)
		self.data = None

	def transform(self, transform):
		self.transform = transform

	def _load(self):
		self.data = pd.read_csv(self.input_path, self.schema)

	def _apply(self):
		# actually apply the transformation to the dataset
		self.data = self.data[self.data.apply(self.transform, axis=1)]

	def compute(self):
		self._apply()

	def _write(self):
		self.data.to_csv(self.output_path)
