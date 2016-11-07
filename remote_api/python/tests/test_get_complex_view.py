import unittest
import lynx


class TestGetComplexView(unittest.TestCase):

  def test_guids_segmentations_and_scalars(self):
    p = lynx.LynxKite().new_project()
    p.exampleGraph()
    md = p.metadata()
    vs = md.vertex_set_id()
    eb = md.edge_bundle_id()
    centers = p.centers(2, vs)
    req = dict(
        vertexSets=[
            dict(
                vertexSetId=vs,
                sampleSmearEdgeBundleId=eb,
                mode='sampled',
                filters=[],
                centralVertexIds=centers,
                attrs=[],
                xBucketingAttributeId='',
                yBucketingAttributeId='',
                xNumBuckets=1,
                yNumBuckets=1,
                radius=1,
                xAxisOptions=dict(logarithmic=False),
                yAxisOptions=dict(logarithmic=False),
                sampleSize=50000,
                maxSize=10000
            )],
        edgeBundles=[
            dict(
                srcDiagramId='idx[0]',
                dstDiagramId='idx[0]',
                srcIdx=0,
                dstIdx=0,
                edgeBundleId=eb,
                filters=[],
                layout3D=False,
                relativeEdgeDensity=False,
                maxSize=10000,
                edgeWeightId='',
                attrs=[]
            )]
    )
    complex_view = p.get_complex_view(req)
    self.assertEqual(len(complex_view.vertexSets[0].vertices), 3)
    self.assertEqual(complex_view.vertexSets[0].mode, 'sampled')
    self.assertEqual(len(complex_view.edgeBundles[0].edges), 4)


if __name__ == '__main__':
  unittest.main()
