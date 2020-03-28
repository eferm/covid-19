import pandas as pd


class Dataset(object):

	def __init__(self, input_path, output_path, schema=None):
		self.input_path = input_path
		self.output_path = output_path
		self.schema = schema
		self.transformations = []

	def transform(self, transform):
		self.transformations = transform

	def _load(self):
		raise NotImplementedError()

	def _apply(self, transformation):
		# actually apply the transformation to the dataset
		raise NotImplementedError()

	def _write(self):
		raise NotImplementedError()


class PandasDataset(Dataset):

	def __init__(self, input_path, output_path, schema, _read_on_create=False):
		super(PandasDataset, self).__init__(input_path, output_path, schema)
		self.data = None
		if _read_on_create:
			self._load()

	def transform(self, transform):
		self.transformations.append(transform)
		return self

	def load(self):
		self._load()
		return self

	def _load(self):
		if not self.schema:
			self.data = pd.read_csv(self.input_path)
		else:
			self.data = pd.read_csv(self.input_path, self.schema)

	def _apply(self, transformation):
		# apply a single transformation stage to a dataset
		self.data = self.data[self.data.apply(transformation, axis=1)]

	def compute(self):
		for transformation in self.transformations:
			self._apply(transformation)

	def _write(self):
		self.data.to_csv(self.output_path)
