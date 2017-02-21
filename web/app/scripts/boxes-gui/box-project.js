// The '/boxproject' page.
'use strict';

angular.module('biggraph')
  .controller('BoxProjectCtrl', function ($scope, $routeParams) {

  $scope.project = $routeParams.project;

  $scope.diagram = {
    'boxes' : [ {
      'id' : 'Example Graph1',
      'category' : 'Structure operations',
      'operation' : 'Example Graph',
      'parameters' : { },
      'x' : 10.0,
      'y' : 100.0,
      'inputs' : [ ],
      'outputs' : [ {
        'id' : 'project',
        'kind' : 'project'
      }, {
        'id': 'krumpli',
        'kind': 'project'
      }, {
        'id': 'nudli',
        'kind': 'project'
      } ],
      'status' : {
        'enabled' : true,
        'disabledReason' : ''
      }
    }, {
      'id' : 'PageRank1',
      'category' : 'Graph metrics',
      'operation' : 'PageRank',
      'parameters' : {
        'name' : 'pagerank',
        'weights' : '!no weight',
        'direction' : 'all edges',
        'iterations' : '5',
        'damping' : '0.85'
      },
      'x' : 10.0,
      'y' : 200.0,
      'inputs' : [ {
        'id' : 'project',
        'kind' : 'project'
      } ],
      'outputs' : [ {
        'id' : 'project',
        'kind' : 'project'
      } ],
      'status' : {
        'enabled' : false,
        'disabledReason' : 'Disconnected.'
      }
    } ],
    'arrows' : [ {
      'src' : {
        'box' : 'Example Graph1',
        'id' : 'project',
        'kind' : 'project'
      },
      'dst' : {
        'box' : 'PageRank1',
        'id' : 'project',
        'kind' : 'project'
      }
    } ],
    'states' : [ {
      'box' : 'Example Graph1',
      'output' : 'project',
      'kind' : 'project',
      'state': {
        '$resolved': true,
        'edgeAttributes': [
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': '136d7f66-da4f-3be3-b739-c50dbb7f2e8b',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'comment',
                'typeName': 'String'
            },
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': '0d8b31ba-a515-3c3e-9e6b-7a8ad38504bf',
                'isInternal': false,
                'isNumeric': true,
                'metadata': {},
                'note': '',
                'title': 'weight',
                'typeName': 'Double'
            }
        ],
        'edgeBundle': '361be094-7790-3583-8769-78270fc2d166',
        'name': 'x123',
        'notes': '',
        'readACL': 'fake',
        'redoOp': '',
        'scalars': [
            {
                'computeProgress': 1.0,
                'computedValue': {
                    '$resolved': true,
                    'defined': true,
                    'double': 4.0,
                    'string': '4'
                },
                'id': '2be1c70f-777f-3277-a797-c18f0d1890d4',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'edge_count',
                'typeName': 'Long'
            },
            {
                'computeProgress': 1.0,
                'computedValue': {
                    'defined': true,
                    'string': 'Hello world! \ud83d\ude00 ',
                    '$resolved': true,
                },
                'id': 'dd8c95e6-27a5-3752-893a-91fcc4b0becb',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'greeting',
                'typeName': 'String'
            },
            {
                'computeProgress': 1.0,
                'computedValue': {
                    '$resolved': true,
                    'defined': true,
                    'double': 4.0,
                    'string': '4'
                },
                'id': '660c4ea3-8433-313b-817b-b2322d22bec9',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'vertex_count',
                'typeName': 'Long'
            }
        ],
        'segmentations': [],
        'undoOp': 'PageRank',
        'vertexAttributes': [
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': 'f028d190-61c7-3c0c-b44d-df8ff81e879b',
                'isInternal': false,
                'isNumeric': true,
                'metadata': {},
                'note': '',
                'title': 'age',
                'typeName': 'Double'
            },
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': 'b4094781-a97e-3ff4-866a-c3b9edd40588',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'gender',
                'typeName': 'String'
            },
            {
                'canBucket': false,
                'canFilter': true,
                'computeProgress': 0.0,
                'id': '38f3a510-d3f7-36ea-a65a-182b7a11f22f',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'id',
                'typeName': 'ID'
            },
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': '48294e0c-d2a6-3d3f-a5a3-f1de0f40407b',
                'isInternal': false,
                'isNumeric': true,
                'metadata': {
                    'icon': 'money_bag'
                },
                'note': '',
                'title': 'income',
                'typeName': 'Double'
            },
            {
                'canBucket': false,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': '36cb61cb-cefe-364a-ad80-a1754822a3fe',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {
                    'icon': 'paw_prints'
                },
                'note': '',
                'title': 'location',
                'typeName': '(Double, Double)'
            },
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': '494d95c8-52ed-30ea-bd2f-23a28e5f96e5',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'name',
                'typeName': 'String'
            },
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 0.0,
                'id': '33f6398d-af06-3a65-9340-053f2511f100',
                'isInternal': false,
                'isNumeric': true,
                'metadata': {},
                'note': '<help-popup href=\'PageRank\'></help-popup>',
                'title': 'page_rank',
                'typeName': 'Double'
            }
        ],
        'vertexSet': 'a9c90a6a-f143-3c67-8980-93b8c4122be1',
        'writeACL': 'fake'
      }
    }, {
      'box' : 'PageRank1',
      'output' : 'project',
      'kind' : 'project',
      'state' : {
        '$resolved': true,
        'edgeAttributes': [
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': '136d7f66-da4f-3be3-b739-c50dbb7f2e8b',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'comment',
                'typeName': 'String'
            },
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': '0d8b31ba-a515-3c3e-9e6b-7a8ad38504bf',
                'isInternal': false,
                'isNumeric': true,
                'metadata': {},
                'note': '',
                'title': 'weight',
                'typeName': 'Double'
            }
        ],
        'edgeBundle': '361be094-7790-3583-8769-78270fc2d166',
        'name': 'x123',
        'notes': '',
        'readACL': 'fake',
        'redoOp': '',
        'scalars': [
            {
                'computeProgress': 1.0,
                'computedValue': {
                    '$resolved': true,
                    'defined': true,
                    'double': 4.0,
                    'string': '4'
                },
                'id': '2be1c70f-777f-3277-a797-c18f0d1890d4',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'edge_count',
                'typeName': 'Long'
            },
            {
                'computeProgress': 1.0,
                'computedValue': {
                    '$resolved': true,
                    'defined': true,
                    'string': 'Hello world! \ud83d\ude00 '
                },
                'id': 'dd8c95e6-27a5-3752-893a-91fcc4b0becb',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'greeting',
                'typeName': 'String'
            },
            {
                'computeProgress': 1.0,
                'computedValue': {
                    '$resolved': true,
                    'defined': true,
                    'double': 4.0,
                    'string': '4'
                },
                'id': '660c4ea3-8433-313b-817b-b2322d22bec9',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'vertex_count',
                'typeName': 'Long'
            }
        ],
        'segmentations': [],
        'undoOp': 'PageRank',
        'vertexAttributes': [
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': 'f028d190-61c7-3c0c-b44d-df8ff81e879b',
                'isInternal': false,
                'isNumeric': true,
                'metadata': {},
                'note': '',
                'title': 'age',
                'typeName': 'Double'
            },
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': 'b4094781-a97e-3ff4-866a-c3b9edd40588',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'gender',
                'typeName': 'String'
            },
            {
                'canBucket': false,
                'canFilter': true,
                'computeProgress': 0.0,
                'id': '38f3a510-d3f7-36ea-a65a-182b7a11f22f',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'id',
                'typeName': 'ID'
            },
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': '48294e0c-d2a6-3d3f-a5a3-f1de0f40407b',
                'isInternal': false,
                'isNumeric': true,
                'metadata': {
                    'icon': 'money_bag'
                },
                'note': '',
                'title': 'income',
                'typeName': 'Double'
            },
            {
                'canBucket': false,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': '36cb61cb-cefe-364a-ad80-a1754822a3fe',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {
                    'icon': 'paw_prints'
                },
                'note': '',
                'title': 'location',
                'typeName': '(Double, Double)'
            },
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 1.0,
                'id': '494d95c8-52ed-30ea-bd2f-23a28e5f96e5',
                'isInternal': false,
                'isNumeric': false,
                'metadata': {},
                'note': '',
                'title': 'name',
                'typeName': 'String'
            },
            {
                'canBucket': true,
                'canFilter': true,
                'computeProgress': 0.0,
                'id': '33f6398d-af06-3a65-9340-053f2511f100',
                'isInternal': false,
                'isNumeric': true,
                'metadata': {},
                'note': '<help-popup href=\'PageRank\'></help-popup>',
                'title': 'page_rank',
                'typeName': 'Double'
            }
        ],
        'vertexSet': 'a9c90a6a-f143-3c67-8980-93b8c4122be1',
        'writeACL': 'fake'



      }
    } ]
  };

  $scope.opCategories = [
        {
            'color': 'blue',
            'icon': '',
            'ops': [
                {
                    'category': 'Edge attribute operations',
                    'description': '',
                    'id': 'Add-constant-edge-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'weight',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': '1',
                            'id': 'value',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Value'
                        },
                        {
                            'defaultValue': 'Double',
                            'id': 'type',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'Double',
                                    'title': 'Double'
                                },
                                {
                                    'id': 'String',
                                    'title': 'String'
                                }
                            ],
                            'title': 'Type'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Add constant edge attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Edge attribute operations',
                    'description': '',
                    'id': 'Add-random-edge-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'random',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': 'Standard Normal',
                            'id': 'dist',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'Standard Normal',
                                    'title': 'Standard Normal'
                                },
                                {
                                    'id': 'Standard Uniform',
                                    'title': 'Standard Uniform'
                                }
                            ],
                            'title': 'Distribution'
                        },
                        {
                            'defaultValue': '-815568669',
                            'id': 'seed',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Seed'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Add random edge attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Edge attribute operations',
                    'description': '',
                    'id': 'Derived-edge-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'output',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Save as'
                        },
                        {
                            'defaultValue': 'double',
                            'id': 'type',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'double',
                                    'title': 'double'
                                },
                                {
                                    'id': 'string',
                                    'title': 'string'
                                },
                                {
                                    'id': 'vector of doubles',
                                    'title': 'vector of doubles'
                                },
                                {
                                    'id': 'vector of strings',
                                    'title': 'vector of strings'
                                }
                            ],
                            'title': 'Result type'
                        },
                        {
                            'defaultValue': 'true',
                            'id': 'defined_attrs',
                            'kind': 'choice',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'true',
                                    'title': 'true'
                                },
                                {
                                    'id': 'false',
                                    'title': 'false'
                                }
                            ],
                            'title': 'Only run on defined attributes'
                        },
                        {
                            'defaultValue': '1 + 1',
                            'id': 'expr',
                            'kind': 'code',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Value'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Derived edge attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Edge attribute operations',
                    'description': '',
                    'id': 'Discard-edge-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'comment',
                            'id': 'name',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'comment',
                                    'title': 'comment'
                                },
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Discard edge attributes',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Edge attribute operations',
                    'description': '',
                    'id': 'Edge-attribute-to-double',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'comment',
                            'id': 'attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'comment',
                                    'title': 'comment'
                                }
                            ],
                            'title': 'Edge attribute'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Edge attribute to double',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Edge attribute operations',
                    'description': '',
                    'id': 'Edge-attribute-to-string',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'comment',
                            'id': 'attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'comment',
                                    'title': 'comment'
                                },
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Edge attribute'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Edge attribute to string',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Edge attribute operations',
                    'description': '',
                    'id': 'Fill-edge-attribute-with-constant-default-value',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'comment',
                            'id': 'attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'comment',
                                    'title': 'comment'
                                },
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Edge attribute'
                        },
                        {
                            'defaultValue': '',
                            'id': 'def',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Default value'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Fill edge attribute with constant default value',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Edge attribute operations',
                    'description': '',
                    'id': 'Internal-edge-ID-as-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'id',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Internal edge ID as attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Edge attribute operations',
                    'description': '',
                    'id': 'Merge-two-edge-attributes',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'New attribute name'
                        },
                        {
                            'defaultValue': 'comment',
                            'id': 'attr1',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'comment',
                                    'title': 'comment'
                                },
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Primary attribute'
                        },
                        {
                            'defaultValue': 'comment',
                            'id': 'attr2',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'comment',
                                    'title': 'comment'
                                },
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Secondary attribute'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Merge two edge attributes',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                }
            ],
            'title': 'Edge attribute operations'
        },
        {
            'color': 'blue',
            'icon': '',
            'ops': [
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Add-constant-vertex-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': '1',
                            'id': 'value',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Value'
                        },
                        {
                            'defaultValue': 'Double',
                            'id': 'type',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'Double',
                                    'title': 'Double'
                                },
                                {
                                    'id': 'String',
                                    'title': 'String'
                                }
                            ],
                            'title': 'Type'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Add constant vertex attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Add-random-vertex-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'random',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': 'Standard Normal',
                            'id': 'dist',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'Standard Normal',
                                    'title': 'Standard Normal'
                                },
                                {
                                    'id': 'Standard Uniform',
                                    'title': 'Standard Uniform'
                                }
                            ],
                            'title': 'Distribution'
                        },
                        {
                            'defaultValue': '-854131219',
                            'id': 'seed',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Seed'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Add random vertex attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Add-rank-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'ranking',
                            'id': 'rankattr',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Rank attribute name'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'keyattr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Key attribute name'
                        },
                        {
                            'defaultValue': 'ascending',
                            'id': 'order',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'ascending',
                                    'title': 'ascending'
                                },
                                {
                                    'id': 'descending',
                                    'title': 'descending'
                                }
                            ],
                            'title': 'Order'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Add rank attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Derived-vertex-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'output',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Save as'
                        },
                        {
                            'defaultValue': 'double',
                            'id': 'type',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'double',
                                    'title': 'double'
                                },
                                {
                                    'id': 'string',
                                    'title': 'string'
                                },
                                {
                                    'id': 'vector of doubles',
                                    'title': 'vector of doubles'
                                },
                                {
                                    'id': 'vector of strings',
                                    'title': 'vector of strings'
                                }
                            ],
                            'title': 'Result type'
                        },
                        {
                            'defaultValue': 'true',
                            'id': 'defined_attrs',
                            'kind': 'choice',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'true',
                                    'title': 'true'
                                },
                                {
                                    'id': 'false',
                                    'title': 'false'
                                }
                            ],
                            'title': 'Only run on defined attributes'
                        },
                        {
                            'defaultValue': '1 + 1',
                            'id': 'expr',
                            'kind': 'code',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Value'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Derived vertex attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Discard-vertex-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'age',
                            'id': 'name',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Discard vertex attributes',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Fill-with-constant-default-value',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'gender',
                            'id': 'attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Vertex attribute'
                        },
                        {
                            'defaultValue': '',
                            'id': 'def',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Default value'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Fill vertex attribute with constant default value',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Internal-vertex-ID-as-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'id',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Internal vertex ID as attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Lookup-region',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'location',
                            'id': 'position',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'location',
                                    'title': 'location'
                                }
                            ],
                            'title': 'Position'
                        },
                        {
                            'defaultValue': '',
                            'id': 'shapefile',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Shapefile'
                        },
                        {
                            'defaultValue': '',
                            'id': 'attribute',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute from the Shapefile'
                        },
                        {
                            'defaultValue': '',
                            'id': 'output',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Output name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Lookup region',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Merge-two-attributes',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'New attribute name'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'attr1',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Primary attribute'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'attr2',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Secondary attribute'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Merge two vertex attributes',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Vertex-attribute-to-double',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'gender',
                            'id': 'attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                }
                            ],
                            'title': 'Vertex attribute'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Vertex attribute to double',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Vertex-attribute-to-string',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'age',
                            'id': 'attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Vertex attribute'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Vertex attribute to string',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Vertex attribute operations',
                    'description': '',
                    'id': 'Vertex-attributes-to-position',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'position',
                            'id': 'output',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Save as'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'x',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'X or latitude'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'y',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Y or longitude'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Vertex attributes to position',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                }
            ],
            'title': 'Vertex attribute operations'
        },
        {
            'color': 'green',
            'icon': 'th-large',
            'ops': [
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Combine-segmentations',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'New segmentation name'
                        },
                        {
                            'defaultValue': '',
                            'id': 'segmentations',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [],
                            'title': 'Segmentations'
                        }
                    ],
                    'status': {
                        'disabledReason': 'No segmentations',
                        'enabled': false
                    },
                    'title': 'Combine segmentations',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Connected-components',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'connected_components',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Segmentation name'
                        },
                        {
                            'defaultValue': 'ignore directions',
                            'id': 'directions',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'ignore directions',
                                    'title': 'ignore directions'
                                },
                                {
                                    'id': 'require both directions',
                                    'title': 'require both directions'
                                }
                            ],
                            'title': 'Edge direction'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Connected components',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Copy-graph-into-a-segmentation',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'self_as_segmentation',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Segmentation name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Copy graph into a segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Discard-segmentation',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Name'
                        }
                    ],
                    'status': {
                        'disabledReason': 'No segmentations',
                        'enabled': false
                    },
                    'title': 'Discard segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Enumerate-triangles',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'triangles',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Segmentation name'
                        },
                        {
                            'defaultValue': 'true',
                            'id': 'bothdir',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'true',
                                    'title': 'true'
                                },
                                {
                                    'id': 'false',
                                    'title': 'false'
                                }
                            ],
                            'title': 'Edges required in both directions'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Enumerate triangles',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Find-infocom-communities',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'maximal_cliques',
                            'id': 'cliques_name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Name for maximal cliques segmentation'
                        },
                        {
                            'defaultValue': 'communities',
                            'id': 'communities_name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Name for communities segmentation'
                        },
                        {
                            'defaultValue': 'true',
                            'id': 'bothdir',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'true',
                                    'title': 'true'
                                },
                                {
                                    'id': 'false',
                                    'title': 'false'
                                }
                            ],
                            'title': 'Edges required in cliques in both directions'
                        },
                        {
                            'defaultValue': '3',
                            'id': 'min_cliques',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Minimum clique size'
                        },
                        {
                            'defaultValue': '0.6',
                            'id': 'adjacency_threshold',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Adjacency threshold for clique overlaps'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Find infocom communities',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Maximal-cliques',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'maximal_cliques',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Segmentation name'
                        },
                        {
                            'defaultValue': 'true',
                            'id': 'bothdir',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'true',
                                    'title': 'true'
                                },
                                {
                                    'id': 'false',
                                    'title': 'false'
                                }
                            ],
                            'title': 'Edges required in both directions'
                        },
                        {
                            'defaultValue': '3',
                            'id': 'min',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Minimum clique size'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Maximal cliques',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Modular-clustering',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'modular_clusters',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Segmentation name'
                        },
                        {
                            'defaultValue': '!no weight',
                            'id': 'weights',
                            'kind': 'choice',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!no weight',
                                    'title': 'no weight'
                                },
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Weight attribute'
                        },
                        {
                            'defaultValue': '30',
                            'id': 'max-iterations',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Maximum number of iterations to do'
                        },
                        {
                            'defaultValue': '0.001',
                            'id': 'min-increment-per-iteration',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Minimal modularity increment in an iteration to keep going'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Modular clustering',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Segment-by-double-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'bucketing',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Segmentation name'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Attribute'
                        },
                        {
                            'defaultValue': '',
                            'id': 'interval-size',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Interval size'
                        },
                        {
                            'defaultValue': 'no',
                            'id': 'overlap',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'no',
                                    'title': 'no'
                                },
                                {
                                    'id': 'yes',
                                    'title': 'yes'
                                }
                            ],
                            'title': 'Overlap'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Segment by double attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Segment-by-event-sequence',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Target segmentation name'
                        },
                        {
                            'defaultValue': 'Attribute: gender',
                            'id': 'location',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'Attribute: gender',
                                    'title': 'Attribute: gender'
                                },
                                {
                                    'id': 'Attribute: name',
                                    'title': 'Attribute: name'
                                }
                            ],
                            'title': 'Location'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'time-attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Time attribute'
                        },
                        {
                            'defaultValue': 'continuous',
                            'id': 'algorithm',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'continuous',
                                    'title': 'Take continuous event sequences'
                                },
                                {
                                    'id': 'with-gaps',
                                    'title': 'Allow gaps in event sequences'
                                }
                            ],
                            'title': 'Algorithm'
                        },
                        {
                            'defaultValue': '2',
                            'id': 'sequence-length',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Sequence length'
                        },
                        {
                            'defaultValue': '',
                            'id': 'time-window-step',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Time window step'
                        },
                        {
                            'defaultValue': '',
                            'id': 'time-window-length',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Time window length'
                        }
                    ],
                    'status': {
                        'disabledReason': 'Must be run on a segmentation',
                        'enabled': false
                    },
                    'title': 'Segment by event sequence',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Segment-by-interval',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'bucketing',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Segmentation name'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'begin_attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Begin attribute'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'end_attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'End attribute'
                        },
                        {
                            'defaultValue': '',
                            'id': 'interval_size',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Interval size'
                        },
                        {
                            'defaultValue': 'no',
                            'id': 'overlap',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'no',
                                    'title': 'no'
                                },
                                {
                                    'id': 'yes',
                                    'title': 'yes'
                                }
                            ],
                            'title': 'Overlap'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Segment by interval',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Create segmentation',
                    'description': '',
                    'id': 'Segment-by-string-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'bucketing',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Segmentation name'
                        },
                        {
                            'defaultValue': 'gender',
                            'id': 'attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                }
                            ],
                            'title': 'Attribute'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Segment by string attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                }
            ],
            'title': 'Create segmentation'
        },
        {
            'color': 'magenta',
            'icon': 'globe',
            'ops': [
                {
                    'category': 'Global operations',
                    'description': '',
                    'id': 'Aggregate-edge-attribute-globally',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'prefix',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Generated name prefix'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-comment',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                }
                            ],
                            'title': 'comment'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-weight',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                }
                            ],
                            'title': 'weight'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Aggregate edge attribute globally',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Global operations',
                    'description': '',
                    'id': 'Aggregate-vertex-attribute-globally',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'prefix',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Generated name prefix'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-age',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                }
                            ],
                            'title': 'age'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-gender',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                }
                            ],
                            'title': 'gender'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-id',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                }
                            ],
                            'title': 'id'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-income',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                }
                            ],
                            'title': 'income'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-location',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                }
                            ],
                            'title': 'location'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-name',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                }
                            ],
                            'title': 'name'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-page_rank',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                }
                            ],
                            'title': 'page_rank'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Aggregate vertex attribute globally',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Global operations',
                    'description': '',
                    'id': 'Compare-segmentation-edges',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'golden',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Golden segmentation'
                        },
                        {
                            'defaultValue': '',
                            'id': 'test',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Test segmentation'
                        }
                    ],
                    'status': {
                        'disabledReason': 'NOPE(tm)',
                        'enabled': false
                    },
                    'title': 'Compare segmentation edges',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Global operations',
                    'description': '',
                    'id': 'Correlate-two-attributes',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'age',
                            'id': 'attrA',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'First attribute'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'attrB',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Second attribute'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Correlate two attributes',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Global operations',
                    'description': '',
                    'id': 'Derive-scalar',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'output',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Save as'
                        },
                        {
                            'defaultValue': 'double',
                            'id': 'type',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'double',
                                    'title': 'double'
                                },
                                {
                                    'id': 'string',
                                    'title': 'string'
                                }
                            ],
                            'title': 'Result type'
                        },
                        {
                            'defaultValue': '1 + 1',
                            'id': 'expr',
                            'kind': 'code',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Value'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Derive scalar',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Global operations',
                    'description': '',
                    'id': 'Discard-scalar',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'edge_count',
                            'id': 'name',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'edge_count',
                                    'title': 'edge_count'
                                },
                                {
                                    'id': 'greeting',
                                    'title': 'greeting'
                                },
                                {
                                    'id': 'vertex_count',
                                    'title': 'vertex_count'
                                }
                            ],
                            'title': 'Name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Discard scalars',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Global operations',
                    'description': '',
                    'id': 'Weighted-aggregate-edge-attribute-globally',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'prefix',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Generated name prefix'
                        },
                        {
                            'defaultValue': 'weight',
                            'id': 'weight',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Weight'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-comment',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                }
                            ],
                            'title': 'comment'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-weight',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'weighted_average',
                                    'title': 'weighted_average'
                                },
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                },
                                {
                                    'id': 'weighted_sum',
                                    'title': 'weighted_sum'
                                }
                            ],
                            'title': 'weight'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Weighted aggregate edge attribute globally',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Global operations',
                    'description': '',
                    'id': 'Weighted-aggregate-vertex-attribute-globally',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'prefix',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Generated name prefix'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'weight',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Weight'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-age',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'weighted_average',
                                    'title': 'weighted_average'
                                },
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                },
                                {
                                    'id': 'weighted_sum',
                                    'title': 'weighted_sum'
                                }
                            ],
                            'title': 'age'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-gender',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                }
                            ],
                            'title': 'gender'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-id',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                }
                            ],
                            'title': 'id'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-income',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'weighted_average',
                                    'title': 'weighted_average'
                                },
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                },
                                {
                                    'id': 'weighted_sum',
                                    'title': 'weighted_sum'
                                }
                            ],
                            'title': 'income'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-location',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                }
                            ],
                            'title': 'location'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-name',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                }
                            ],
                            'title': 'name'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-page_rank',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'weighted_average',
                                    'title': 'weighted_average'
                                },
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                },
                                {
                                    'id': 'weighted_sum',
                                    'title': 'weighted_sum'
                                }
                            ],
                            'title': 'page_rank'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Weighted aggregate vertex attribute globally',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                }
            ],
            'title': 'Global operations'
        },
        {
            'color': 'green',
            'icon': 'stats',
            'ops': [
                {
                    'category': 'Graph metrics',
                    'description': '',
                    'id': 'Approximate-clustering-coefficient',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'clustering_coefficient',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': '8',
                            'id': 'bits',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Precision'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Approximate clustering coefficient',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Graph metrics',
                    'description': '',
                    'id': 'Approximate-embeddedness',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'embeddedness',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': '8',
                            'id': 'bits',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Precision'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Approximate embeddedness',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Graph metrics',
                    'description': '',
                    'id': 'Centrality',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'centrality',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': '10',
                            'id': 'maxDiameter',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Maximal diameter to check'
                        },
                        {
                            'defaultValue': 'Harmonic',
                            'id': 'algorithm',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'Harmonic',
                                    'title': 'Harmonic'
                                },
                                {
                                    'id': 'Lin',
                                    'title': 'Lin'
                                },
                                {
                                    'id': 'Average distance',
                                    'title': 'Average distance'
                                }
                            ],
                            'title': 'Centrality type'
                        },
                        {
                            'defaultValue': '8',
                            'id': 'bits',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Precision'
                        },
                        {
                            'defaultValue': 'outgoing edges',
                            'id': 'direction',
                            'kind': 'choice',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'outgoing edges',
                                    'title': 'outgoing edges'
                                },
                                {
                                    'id': 'incoming edges',
                                    'title': 'incoming edges'
                                },
                                {
                                    'id': 'all edges',
                                    'title': 'all edges'
                                }
                            ],
                            'title': 'Direction'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Centrality',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Graph metrics',
                    'description': '',
                    'id': 'Clustering-coefficient',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'clustering_coefficient',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Clustering coefficient',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Graph metrics',
                    'description': '',
                    'id': 'Degree',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'degree',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': 'incoming edges',
                            'id': 'direction',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'incoming edges',
                                    'title': 'incoming edges'
                                },
                                {
                                    'id': 'outgoing edges',
                                    'title': 'outgoing edges'
                                },
                                {
                                    'id': 'all edges',
                                    'title': 'all edges'
                                },
                                {
                                    'id': 'symmetric edges',
                                    'title': 'symmetric edges'
                                },
                                {
                                    'id': 'in-neighbors',
                                    'title': 'in-neighbors'
                                },
                                {
                                    'id': 'out-neighbors',
                                    'title': 'out-neighbors'
                                },
                                {
                                    'id': 'all neighbors',
                                    'title': 'all neighbors'
                                },
                                {
                                    'id': 'symmetric neighbors',
                                    'title': 'symmetric neighbors'
                                }
                            ],
                            'title': 'Count'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Degree',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Graph metrics',
                    'description': '',
                    'id': 'Dispersion',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'dispersion',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Dispersion',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Graph metrics',
                    'description': '',
                    'id': 'Embeddedness',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'embeddedness',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Embeddedness',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Graph metrics',
                    'description': '',
                    'id': 'Find-vertex-coloring',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'color',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Find vertex coloring',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Graph metrics',
                    'description': '',
                    'id': 'PageRank',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'page_rank',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': '!no weight',
                            'id': 'weights',
                            'kind': 'choice',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!no weight',
                                    'title': 'no weight'
                                },
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Weight attribute'
                        },
                        {
                            'defaultValue': '5',
                            'id': 'iterations',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Number of iterations'
                        },
                        {
                            'defaultValue': '0.85',
                            'id': 'damping',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Damping factor'
                        },
                        {
                            'defaultValue': 'outgoing edges',
                            'id': 'direction',
                            'kind': 'choice',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'outgoing edges',
                                    'title': 'outgoing edges'
                                },
                                {
                                    'id': 'incoming edges',
                                    'title': 'incoming edges'
                                },
                                {
                                    'id': 'all edges',
                                    'title': 'all edges'
                                }
                            ],
                            'title': 'Direction'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'PageRank',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Graph metrics',
                    'description': '',
                    'id': 'Shortest-path',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'shortest_distance',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': '!unit distances',
                            'id': 'edge_distance',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!unit distances',
                                    'title': 'unit distances'
                                },
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Edge distance attribute'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'starting_distance',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Starting distance attribute'
                        },
                        {
                            'defaultValue': '10',
                            'id': 'iterations',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Maximum number of iterations'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Shortest path',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                }
            ],
            'title': 'Graph metrics'
        },
        {
            'color': 'yellow',
            'icon': 'import',
            'ops': [
                {
                    'category': 'Import operations',
                    'description': '',
                    'id': 'Hash-vertex-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'age',
                            'id': 'attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Vertex attribute'
                        },
                        {
                            'defaultValue': 'SECRET(wUDFXg4tqcgMkz2)',
                            'id': 'salt',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Salt'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Hash vertex attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Import operations',
                    'description': '',
                    'id': 'Import-edge-attributes',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'table',
                            'kind': 'table',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'edges',
                                    'title': 'edges'
                                },
                                {
                                    'id': 'edge_attributes',
                                    'title': 'edge_attributes'
                                },
                                {
                                    'id': 'vertices',
                                    'title': 'vertices'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edge_attributes',
                                    'title': 'meta|edge_attributes (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edges',
                                    'title': 'meta|edges (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|vertices',
                                    'title': 'meta|vertices (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edge_attributes',
                                    'title': 'x123|edge_attributes (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edges',
                                    'title': 'x123|edges (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|vertices',
                                    'title': 'x123|vertices (2017-02-17 13:10 CET)'
                                }
                            ],
                            'title': 'Table to import from'
                        },
                        {
                            'defaultValue': '!unset',
                            'id': 'id-attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!unset',
                                    'title': ''
                                },
                                {
                                    'id': 'comment',
                                    'title': 'comment'
                                }
                            ],
                            'title': 'Edge attribute'
                        },
                        {
                            'defaultValue': '',
                            'id': 'id-column',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'ID column'
                        },
                        {
                            'defaultValue': '',
                            'id': 'prefix',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Name prefix for the imported edge attributes'
                        },
                        {
                            'defaultValue': 'true',
                            'id': 'unique-keys',
                            'kind': 'choice',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'true',
                                    'title': 'true'
                                },
                                {
                                    'id': 'false',
                                    'title': 'false'
                                }
                            ],
                            'title': 'Assert unique edge attribute values'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Import edge attributes',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Import operations',
                    'description': '',
                    'id': 'Import-edges-for-existing-vertices',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'table',
                            'kind': 'table',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'edges',
                                    'title': 'edges'
                                },
                                {
                                    'id': 'edge_attributes',
                                    'title': 'edge_attributes'
                                },
                                {
                                    'id': 'vertices',
                                    'title': 'vertices'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edge_attributes',
                                    'title': 'meta|edge_attributes (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edges',
                                    'title': 'meta|edges (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|vertices',
                                    'title': 'meta|vertices (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edge_attributes',
                                    'title': 'x123|edge_attributes (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edges',
                                    'title': 'x123|edges (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|vertices',
                                    'title': 'x123|vertices (2017-02-17 13:10 CET)'
                                }
                            ],
                            'title': 'Table to import from'
                        },
                        {
                            'defaultValue': '!unset',
                            'id': 'attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!unset',
                                    'title': ''
                                },
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Vertex ID attribute'
                        },
                        {
                            'defaultValue': '',
                            'id': 'src',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Source ID column'
                        },
                        {
                            'defaultValue': '',
                            'id': 'dst',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Destination ID column'
                        }
                    ],
                    'status': {
                        'disabledReason': 'Edges already exist.',
                        'enabled': false
                    },
                    'title': 'Import edges for existing vertices',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Import operations',
                    'description': '',
                    'id': 'Import-project-as-segmentation',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '!checkpoint(1487334221263,meta)',
                            'id': 'them',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!checkpoint(1487334221263,meta)',
                                    'title': 'meta (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)',
                                    'title': 'x123 (2017-02-17 13:10 CET)'
                                }
                            ],
                            'title': 'Other project\'s name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Import project as segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Import operations',
                    'description': '',
                    'id': 'Import-segmentation',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'table',
                            'kind': 'table',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'edges',
                                    'title': 'edges'
                                },
                                {
                                    'id': 'edge_attributes',
                                    'title': 'edge_attributes'
                                },
                                {
                                    'id': 'vertices',
                                    'title': 'vertices'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edge_attributes',
                                    'title': 'meta|edge_attributes (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edges',
                                    'title': 'meta|edges (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|vertices',
                                    'title': 'meta|vertices (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edge_attributes',
                                    'title': 'x123|edge_attributes (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edges',
                                    'title': 'x123|edges (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|vertices',
                                    'title': 'x123|vertices (2017-02-17 13:10 CET)'
                                }
                            ],
                            'title': 'Table to import from'
                        },
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Name of new segmentation'
                        },
                        {
                            'defaultValue': '!unset',
                            'id': 'base-id-attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!unset',
                                    'title': ''
                                },
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Vertex ID attribute'
                        },
                        {
                            'defaultValue': '',
                            'id': 'base-id-column',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Vertex ID column'
                        },
                        {
                            'defaultValue': '',
                            'id': 'seg-id-column',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Segment ID column'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Import segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Import operations',
                    'description': '',
                    'id': 'Import-segmentation-links',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Import segmentation links',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Import operations',
                    'description': '',
                    'id': 'Import-vertex-attributes',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'table',
                            'kind': 'table',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'edges',
                                    'title': 'edges'
                                },
                                {
                                    'id': 'edge_attributes',
                                    'title': 'edge_attributes'
                                },
                                {
                                    'id': 'vertices',
                                    'title': 'vertices'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edge_attributes',
                                    'title': 'meta|edge_attributes (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edges',
                                    'title': 'meta|edges (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|vertices',
                                    'title': 'meta|vertices (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edge_attributes',
                                    'title': 'x123|edge_attributes (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edges',
                                    'title': 'x123|edges (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|vertices',
                                    'title': 'x123|vertices (2017-02-17 13:10 CET)'
                                }
                            ],
                            'title': 'Table to import from'
                        },
                        {
                            'defaultValue': '!unset',
                            'id': 'id-attr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!unset',
                                    'title': ''
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                }
                            ],
                            'title': 'Vertex attribute'
                        },
                        {
                            'defaultValue': '',
                            'id': 'id-column',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'ID column'
                        },
                        {
                            'defaultValue': '',
                            'id': 'prefix',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Name prefix for the imported vertex attributes'
                        },
                        {
                            'defaultValue': 'true',
                            'id': 'unique-keys',
                            'kind': 'choice',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'true',
                                    'title': 'true'
                                },
                                {
                                    'id': 'false',
                                    'title': 'false'
                                }
                            ],
                            'title': 'Assert unique vertex attribute values'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Import vertex attributes',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Import operations',
                    'description': '',
                    'id': 'Import-vertices',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'table',
                            'kind': 'table',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'edges',
                                    'title': 'edges'
                                },
                                {
                                    'id': 'edge_attributes',
                                    'title': 'edge_attributes'
                                },
                                {
                                    'id': 'vertices',
                                    'title': 'vertices'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edge_attributes',
                                    'title': 'meta|edge_attributes (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edges',
                                    'title': 'meta|edges (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|vertices',
                                    'title': 'meta|vertices (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edge_attributes',
                                    'title': 'x123|edge_attributes (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edges',
                                    'title': 'x123|edges (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|vertices',
                                    'title': 'x123|vertices (2017-02-17 13:10 CET)'
                                }
                            ],
                            'title': 'Table to import from'
                        },
                        {
                            'defaultValue': 'id',
                            'id': 'id-attr',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Save internal ID as'
                        }
                    ],
                    'status': {
                        'disabledReason': 'Vertices already exist.',
                        'enabled': false
                    },
                    'title': 'Import vertices',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Import operations',
                    'description': '',
                    'id': 'Import-vertices-and-edges-from-a-single-table',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'table',
                            'kind': 'table',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'edges',
                                    'title': 'edges'
                                },
                                {
                                    'id': 'edge_attributes',
                                    'title': 'edge_attributes'
                                },
                                {
                                    'id': 'vertices',
                                    'title': 'vertices'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edge_attributes',
                                    'title': 'meta|edge_attributes (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|edges',
                                    'title': 'meta|edges (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487334221263,meta)|vertices',
                                    'title': 'meta|vertices (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edge_attributes',
                                    'title': 'x123|edge_attributes (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|edges',
                                    'title': 'x123|edges (2017-02-17 13:10 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)|vertices',
                                    'title': 'x123|vertices (2017-02-17 13:10 CET)'
                                }
                            ],
                            'title': 'Table to import from'
                        },
                        {
                            'defaultValue': '',
                            'id': 'src',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Source ID column'
                        },
                        {
                            'defaultValue': '',
                            'id': 'dst',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Destination ID column'
                        }
                    ],
                    'status': {
                        'disabledReason': 'Vertices already exist.',
                        'enabled': false
                    },
                    'title': 'Import vertices and edges from a single table',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                }
            ],
            'title': 'Import operations'
        },
        {
            'color': 'pink ',
            'icon': 'knight',
            'ops': [
                {
                    'category': 'Machine learning operations',
                    'description': '',
                    'id': 'Classify-vertices-with-a-model',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'The name of the attribute of the classifications'
                        },
                        {
                            'defaultValue': '',
                            'id': 'model',
                            'kind': 'model',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'payload': {
                                'attrs': [
                                    {
                                        'id': 'age',
                                        'title': 'age'
                                    },
                                    {
                                        'id': 'income',
                                        'title': 'income'
                                    },
                                    {
                                        'id': 'page_rank',
                                        'title': 'page_rank'
                                    }
                                ],
                                'models': []
                            },
                            'title': 'The parameters of the model'
                        }
                    ],
                    'status': {
                        'disabledReason': 'No classification models.',
                        'enabled': false
                    },
                    'title': 'Classify vertices with a model',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Machine learning operations',
                    'description': '',
                    'id': 'Predict-from-model',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'The name of the attribute of the predictions'
                        },
                        {
                            'defaultValue': '',
                            'id': 'model',
                            'kind': 'model',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'payload': {
                                'attrs': [
                                    {
                                        'id': 'age',
                                        'title': 'age'
                                    },
                                    {
                                        'id': 'income',
                                        'title': 'income'
                                    },
                                    {
                                        'id': 'page_rank',
                                        'title': 'page_rank'
                                    }
                                ],
                                'models': []
                            },
                            'title': 'The parameters of the model'
                        }
                    ],
                    'status': {
                        'disabledReason': 'No regression models.',
                        'enabled': false
                    },
                    'title': 'Predict from model',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Machine learning operations',
                    'description': '',
                    'id': 'Predict-vertex-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'age',
                            'id': 'label',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Attribute to predict'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'features',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Predictors'
                        },
                        {
                            'defaultValue': 'Linear regression',
                            'id': 'method',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'Linear regression',
                                    'title': 'Linear regression'
                                },
                                {
                                    'id': 'Ridge regression',
                                    'title': 'Ridge regression'
                                },
                                {
                                    'id': 'Lasso',
                                    'title': 'Lasso'
                                },
                                {
                                    'id': 'Logistic regression',
                                    'title': 'Logistic regression'
                                },
                                {
                                    'id': 'Naive Bayes',
                                    'title': 'Naive Bayes'
                                },
                                {
                                    'id': 'Decision tree',
                                    'title': 'Decision tree'
                                },
                                {
                                    'id': 'Random forest',
                                    'title': 'Random forest'
                                },
                                {
                                    'id': 'Gradient-boosted trees',
                                    'title': 'Gradient-boosted trees'
                                }
                            ],
                            'title': 'Method'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Predict vertex attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Machine learning operations',
                    'description': '',
                    'id': 'Predict-with-a-neural-network-1st-version',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'age',
                            'id': 'label',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Attribute to predict'
                        },
                        {
                            'defaultValue': '',
                            'id': 'output',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Save as'
                        },
                        {
                            'defaultValue': '!unset',
                            'id': 'features',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': '!unset',
                                    'title': ''
                                },
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Predictors'
                        },
                        {
                            'defaultValue': 'GRU',
                            'id': 'networkLayout',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'GRU',
                                    'title': 'GRU'
                                },
                                {
                                    'id': 'LSTM',
                                    'title': 'LSTM'
                                },
                                {
                                    'id': 'MLP',
                                    'title': 'MLP'
                                }
                            ],
                            'title': 'Network layout'
                        },
                        {
                            'defaultValue': '3',
                            'id': 'networkSize',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Size of the network'
                        },
                        {
                            'defaultValue': '3',
                            'id': 'radius',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Iterations in prediction'
                        },
                        {
                            'defaultValue': 'true',
                            'id': 'hideState',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'true',
                                    'title': 'true'
                                },
                                {
                                    'id': 'false',
                                    'title': 'false'
                                }
                            ],
                            'title': 'Hide own state'
                        },
                        {
                            'defaultValue': '0.0',
                            'id': 'forgetFraction',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Forget fraction'
                        },
                        {
                            'defaultValue': '1.0',
                            'id': 'knownLabelWeight',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Weight for known labels'
                        },
                        {
                            'defaultValue': '50',
                            'id': 'numberOfTrainings',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Number of trainings'
                        },
                        {
                            'defaultValue': '2',
                            'id': 'iterationsInTraining',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Iterations in training'
                        },
                        {
                            'defaultValue': '10',
                            'id': 'subgraphsInTraining',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Subgraphs in training'
                        },
                        {
                            'defaultValue': '10',
                            'id': 'minTrainingVertices',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Minimum training subgraph size'
                        },
                        {
                            'defaultValue': '20',
                            'id': 'maxTrainingVertices',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Maximum training subgraph size'
                        },
                        {
                            'defaultValue': '3',
                            'id': 'trainingRadius',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Radius for training subgraphs'
                        },
                        {
                            'defaultValue': '-959108599',
                            'id': 'seed',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Seed'
                        },
                        {
                            'defaultValue': '0.1',
                            'id': 'learningRate',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Learning rate'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Predict with a neural network 1st version',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Machine learning operations',
                    'description': '',
                    'id': 'Reduce-vertex-attributes-to-two-dimensions',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'reduced_dimension1',
                            'id': 'output_name1',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'First dimension name'
                        },
                        {
                            'defaultValue': 'reduced_dimension2',
                            'id': 'output_name2',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Second dimension name'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'features',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Attributes'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Reduce vertex attributes to two dimensions',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Machine learning operations',
                    'description': '',
                    'id': 'Split-to-train-and-test-set',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'age',
                            'id': 'source',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Source attribute'
                        },
                        {
                            'defaultValue': '0.1',
                            'id': 'test_set_ratio',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Test set ratio'
                        },
                        {
                            'defaultValue': '-420013080',
                            'id': 'seed',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Random seed for test set selection'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Split to train and test set',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Machine learning operations',
                    'description': '',
                    'id': 'Train-a-k-means-clustering-model',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'The name of the model'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'features',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Attributes'
                        },
                        {
                            'defaultValue': '2',
                            'id': 'k',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Number of clusters'
                        },
                        {
                            'defaultValue': '20',
                            'id': 'max-iter',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Maximum number of iterations'
                        },
                        {
                            'defaultValue': '220084618',
                            'id': 'seed',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Seed'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Train a k-means clustering model',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Machine learning operations',
                    'description': '',
                    'id': 'Train-a-logistic-regression-model',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'The name of the model'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'label',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Label'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'features',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Features'
                        },
                        {
                            'defaultValue': '20',
                            'id': 'max-iter',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Maximum number of iterations'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Train a logistic regression model',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Machine learning operations',
                    'description': '',
                    'id': 'Train-linear-regression-model',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'The name of the model'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'label',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Label'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'features',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Features'
                        },
                        {
                            'defaultValue': 'Linear regression',
                            'id': 'method',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'Linear regression',
                                    'title': 'Linear regression'
                                },
                                {
                                    'id': 'Ridge regression',
                                    'title': 'Ridge regression'
                                },
                                {
                                    'id': 'Lasso',
                                    'title': 'Lasso'
                                }
                            ],
                            'title': 'Method'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Train linear regression model',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                }
            ],
            'title': 'Machine learning operations'
        },
        {
            'color': 'green',
            'icon': 'fullscreen',
            'ops': [
                {
                    'category': 'Propagation operations',
                    'description': '',
                    'id': 'Aggregate-edge-attribute-to-vertices',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'edge',
                            'id': 'prefix',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Generated name prefix'
                        },
                        {
                            'defaultValue': 'incoming edges',
                            'id': 'direction',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'incoming edges',
                                    'title': 'incoming edges'
                                },
                                {
                                    'id': 'outgoing edges',
                                    'title': 'outgoing edges'
                                },
                                {
                                    'id': 'all edges',
                                    'title': 'all edges'
                                }
                            ],
                            'title': 'Aggregate on'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-comment',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'majority_50',
                                    'title': 'majority_50'
                                },
                                {
                                    'id': 'majority_100',
                                    'title': 'majority_100'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                }
                            ],
                            'title': 'comment'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-weight',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'weight'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Aggregate edge attribute to vertices',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Propagation operations',
                    'description': '',
                    'id': 'Aggregate-from-segmentation',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Aggregate from segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Propagation operations',
                    'description': '',
                    'id': 'Aggregate-on-neighbors',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'neighborhood',
                            'id': 'prefix',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Generated name prefix'
                        },
                        {
                            'defaultValue': 'incoming edges',
                            'id': 'direction',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'incoming edges',
                                    'title': 'incoming edges'
                                },
                                {
                                    'id': 'outgoing edges',
                                    'title': 'outgoing edges'
                                },
                                {
                                    'id': 'all edges',
                                    'title': 'all edges'
                                },
                                {
                                    'id': 'symmetric edges',
                                    'title': 'symmetric edges'
                                },
                                {
                                    'id': 'in-neighbors',
                                    'title': 'in-neighbors'
                                },
                                {
                                    'id': 'out-neighbors',
                                    'title': 'out-neighbors'
                                },
                                {
                                    'id': 'all neighbors',
                                    'title': 'all neighbors'
                                },
                                {
                                    'id': 'symmetric neighbors',
                                    'title': 'symmetric neighbors'
                                }
                            ],
                            'title': 'Aggregate on'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-age',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'age'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-gender',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'majority_50',
                                    'title': 'majority_50'
                                },
                                {
                                    'id': 'majority_100',
                                    'title': 'majority_100'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                }
                            ],
                            'title': 'gender'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-id',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'id'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-income',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'income'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-location',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'location'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-name',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'majority_50',
                                    'title': 'majority_50'
                                },
                                {
                                    'id': 'majority_100',
                                    'title': 'majority_100'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                }
                            ],
                            'title': 'name'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-page_rank',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'page_rank'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Aggregate on neighbors',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Propagation operations',
                    'description': '',
                    'id': 'Aggregate-to-segmentation',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Aggregate to segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Propagation operations',
                    'description': '',
                    'id': 'Copy-vertex-attributes-from-segmentation',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Copy vertex attributes from segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Propagation operations',
                    'description': '',
                    'id': 'Copy-vertex-attributes-to-segmentation',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Copy vertex attributes to segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Propagation operations',
                    'description': '',
                    'id': 'Weighted-aggregate-edge-attribute-to-vertices',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'edge',
                            'id': 'prefix',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Generated name prefix'
                        },
                        {
                            'defaultValue': 'weight',
                            'id': 'weight',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Weight'
                        },
                        {
                            'defaultValue': 'incoming edges',
                            'id': 'direction',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'incoming edges',
                                    'title': 'incoming edges'
                                },
                                {
                                    'id': 'outgoing edges',
                                    'title': 'outgoing edges'
                                },
                                {
                                    'id': 'all edges',
                                    'title': 'all edges'
                                }
                            ],
                            'title': 'Aggregate on'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-comment',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                }
                            ],
                            'title': 'comment'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-weight',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'weighted_average',
                                    'title': 'weighted_average'
                                },
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                },
                                {
                                    'id': 'weighted_sum',
                                    'title': 'weighted_sum'
                                }
                            ],
                            'title': 'weight'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Weighted aggregate edge attribute to vertices',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Propagation operations',
                    'description': '',
                    'id': 'Weighted-aggregate-from-segmentation',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Weighted aggregate from segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Propagation operations',
                    'description': '',
                    'id': 'Weighted-aggregate-on-neighbors',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'neighborhood',
                            'id': 'prefix',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Generated name prefix'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'weight',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Weight'
                        },
                        {
                            'defaultValue': 'incoming edges',
                            'id': 'direction',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'incoming edges',
                                    'title': 'incoming edges'
                                },
                                {
                                    'id': 'outgoing edges',
                                    'title': 'outgoing edges'
                                },
                                {
                                    'id': 'all edges',
                                    'title': 'all edges'
                                },
                                {
                                    'id': 'symmetric edges',
                                    'title': 'symmetric edges'
                                },
                                {
                                    'id': 'in-neighbors',
                                    'title': 'in-neighbors'
                                },
                                {
                                    'id': 'out-neighbors',
                                    'title': 'out-neighbors'
                                },
                                {
                                    'id': 'all neighbors',
                                    'title': 'all neighbors'
                                },
                                {
                                    'id': 'symmetric neighbors',
                                    'title': 'symmetric neighbors'
                                }
                            ],
                            'title': 'Aggregate on'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-age',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'weighted_average',
                                    'title': 'weighted_average'
                                },
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                },
                                {
                                    'id': 'weighted_sum',
                                    'title': 'weighted_sum'
                                }
                            ],
                            'title': 'age'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-gender',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                }
                            ],
                            'title': 'gender'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-id',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                }
                            ],
                            'title': 'id'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-income',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'weighted_average',
                                    'title': 'weighted_average'
                                },
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                },
                                {
                                    'id': 'weighted_sum',
                                    'title': 'weighted_sum'
                                }
                            ],
                            'title': 'income'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-location',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                }
                            ],
                            'title': 'location'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-name',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                }
                            ],
                            'title': 'name'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-page_rank',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'weighted_average',
                                    'title': 'weighted_average'
                                },
                                {
                                    'id': 'by_max_weight',
                                    'title': 'by_max_weight'
                                },
                                {
                                    'id': 'by_min_weight',
                                    'title': 'by_min_weight'
                                },
                                {
                                    'id': 'weighted_sum',
                                    'title': 'weighted_sum'
                                }
                            ],
                            'title': 'page_rank'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Weighted aggregate on neighbors',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Propagation operations',
                    'description': '',
                    'id': 'Weighted-aggregate-to-segmentation',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Weighted aggregate to segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                }
            ],
            'title': 'Propagation operations'
        },
        {
            'color': 'green',
            'icon': 'book',
            'ops': [
                {
                    'category': 'Specialty operations',
                    'description': '',
                    'id': 'Fingerprinting-based-on-attributes',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'gender',
                            'id': 'leftName',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                }
                            ],
                            'title': 'First ID attribute'
                        },
                        {
                            'defaultValue': 'gender',
                            'id': 'rightName',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                }
                            ],
                            'title': 'Second ID attribute'
                        },
                        {
                            'defaultValue': '!no weight',
                            'id': 'weights',
                            'kind': 'choice',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!no weight',
                                    'title': 'no weight'
                                },
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Edge weights'
                        },
                        {
                            'defaultValue': '1',
                            'id': 'mo',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Minimum overlap'
                        },
                        {
                            'defaultValue': '0.5',
                            'id': 'ms',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Minimum similarity'
                        },
                        {
                            'defaultValue': '',
                            'id': 'extra',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Fingerprinting algorithm additional parameters'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Fingerprinting based on attributes',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Specialty operations',
                    'description': '',
                    'id': 'Fingerprinting-between-project-and-segmentation',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Fingerprinting between project and segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Specialty operations',
                    'description': '',
                    'id': 'Viral-modeling',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Viral modeling',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                }
            ],
            'title': 'Specialty operations'
        },
        {
            'color': 'pink',
            'icon': 'asterisk',
            'ops': [
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Add-reversed-edges',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'distattr',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Distinguishing edge attribute'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Add reversed edges',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Connect-vertices-on-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'age',
                            'id': 'fromAttr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Source attribute'
                        },
                        {
                            'defaultValue': 'age',
                            'id': 'toAttr',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Destination attribute'
                        }
                    ],
                    'status': {
                        'disabledReason': 'Edges already exist.',
                        'enabled': false
                    },
                    'title': 'Connect vertices on attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Copy-edges-to-base-project',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Copy edges to base project',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Copy-edges-to-segmentation',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Copy edges to segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Copy-scalar-from-other-project',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '!checkpoint(1487334221263,meta)',
                            'id': 'sourceProject',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!checkpoint(1487334221263,meta)',
                                    'title': 'meta (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)',
                                    'title': 'x123 (2017-02-17 13:10 CET)'
                                }
                            ],
                            'title': 'Other project\'s name'
                        },
                        {
                            'defaultValue': '',
                            'id': 'sourceScalarName',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Name of the scalar in the other project'
                        },
                        {
                            'defaultValue': '',
                            'id': 'destScalarName',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Name for the scalar in this project'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Copy scalar from other project',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Create-edges-from-co-occurrence',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Create edges from co-occurrence',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Create-edges-from-set-overlaps',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'Edges already exist.',
                        'enabled': false
                    },
                    'title': 'Create edges from set overlaps',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Create-random-edge-bundle',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '10.0',
                            'id': 'degree',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Average degree'
                        },
                        {
                            'defaultValue': '1665872220',
                            'id': 'seed',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Seed'
                        }
                    ],
                    'status': {
                        'disabledReason': 'Edges already exist.',
                        'enabled': false
                    },
                    'title': 'Create random edge bundle',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Create-scale-free-random-edge-bundle',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '10',
                            'id': 'iterations',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Number of iterations'
                        },
                        {
                            'defaultValue': '1.3',
                            'id': 'perIterationMultiplier',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Per iteration edge number multiplier'
                        },
                        {
                            'defaultValue': '-1324826321',
                            'id': 'seed',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Seed'
                        }
                    ],
                    'status': {
                        'disabledReason': 'Edges already exist.',
                        'enabled': false
                    },
                    'title': 'Create scale-free random edge bundle',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Create-segmentation-from-SQL',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Name'
                        },
                        {
                            'defaultValue': 'select * from vertices',
                            'id': 'sql',
                            'kind': 'code',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'SQL'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Create segmentation from SQL',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Create-snowball-sample',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '0.0001',
                            'id': 'ratio',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Fraction of vertices to use as starting points'
                        },
                        {
                            'defaultValue': '3',
                            'id': 'radius',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Radius'
                        },
                        {
                            'defaultValue': 'distance_from_start_point',
                            'id': 'attrName',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Attribute name'
                        },
                        {
                            'defaultValue': '-1634350734',
                            'id': 'seed',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Seed'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Create snowball sample',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Define-segmentation-links-from-matching-attributes',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Define segmentation links from matching attributes',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Discard-edges',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Discard edges',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Discard-loop-edges',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Discard loop edges',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Discard-vertices',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Discard vertices',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Edge-graph',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Edge graph',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Example-Graph',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'Vertices already exist.',
                        'enabled': false
                    },
                    'title': 'Example Graph',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Filter-by-attributes',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'filterva-age',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'age'
                        },
                        {
                            'defaultValue': '',
                            'id': 'filterva-gender',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'gender'
                        },
                        {
                            'defaultValue': '',
                            'id': 'filterva-id',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'id'
                        },
                        {
                            'defaultValue': '',
                            'id': 'filterva-income',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'income'
                        },
                        {
                            'defaultValue': '',
                            'id': 'filterva-location',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'location'
                        },
                        {
                            'defaultValue': '',
                            'id': 'filterva-name',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'name'
                        },
                        {
                            'defaultValue': '',
                            'id': 'filterva-page_rank',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'page_rank'
                        },
                        {
                            'defaultValue': '',
                            'id': 'filterea-comment',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'comment'
                        },
                        {
                            'defaultValue': '',
                            'id': 'filterea-weight',
                            'kind': 'default',
                            'mandatory': false,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'weight'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Filter by attributes',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Grow-segmentation',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Grow segmentation',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Merge-parallel-edges',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'aggregate-comment',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'majority_50',
                                    'title': 'majority_50'
                                },
                                {
                                    'id': 'majority_100',
                                    'title': 'majority_100'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                }
                            ],
                            'title': 'comment'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-weight',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'weight'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Merge parallel edges',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Merge-parallel-edges-by-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'comment',
                            'id': 'key',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'comment',
                                    'title': 'comment'
                                },
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Merge by'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-comment',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'majority_50',
                                    'title': 'majority_50'
                                },
                                {
                                    'id': 'majority_100',
                                    'title': 'majority_100'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                }
                            ],
                            'title': 'comment'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-weight',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'weight'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Merge parallel edges by attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Merge-vertices-by-attribute',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'age',
                            'id': 'key',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'gender',
                                    'title': 'gender'
                                },
                                {
                                    'id': 'id',
                                    'title': 'id'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'location',
                                    'title': 'location'
                                },
                                {
                                    'id': 'name',
                                    'title': 'name'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Match by'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-age',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'age'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-gender',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'majority_50',
                                    'title': 'majority_50'
                                },
                                {
                                    'id': 'majority_100',
                                    'title': 'majority_100'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                }
                            ],
                            'title': 'gender'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-id',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'id'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-income',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'income'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-location',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'location'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-name',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'majority_50',
                                    'title': 'majority_50'
                                },
                                {
                                    'id': 'majority_100',
                                    'title': 'majority_100'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                }
                            ],
                            'title': 'name'
                        },
                        {
                            'defaultValue': '',
                            'id': 'aggregate-page_rank',
                            'kind': 'tag-list',
                            'mandatory': false,
                            'multipleChoice': true,
                            'options': [
                                {
                                    'id': 'average',
                                    'title': 'average'
                                },
                                {
                                    'id': 'count',
                                    'title': 'count'
                                },
                                {
                                    'id': 'count_distinct',
                                    'title': 'count_distinct'
                                },
                                {
                                    'id': 'count_most_common',
                                    'title': 'count_most_common'
                                },
                                {
                                    'id': 'first',
                                    'title': 'first'
                                },
                                {
                                    'id': 'max',
                                    'title': 'max'
                                },
                                {
                                    'id': 'median',
                                    'title': 'median'
                                },
                                {
                                    'id': 'min',
                                    'title': 'min'
                                },
                                {
                                    'id': 'most_common',
                                    'title': 'most_common'
                                },
                                {
                                    'id': 'set',
                                    'title': 'set'
                                },
                                {
                                    'id': 'std_deviation',
                                    'title': 'std_deviation'
                                },
                                {
                                    'id': 'sum',
                                    'title': 'sum'
                                },
                                {
                                    'id': 'vector',
                                    'title': 'vector'
                                }
                            ],
                            'title': 'page_rank'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Merge vertices by attribute',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Metagraph',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '1487935324288',
                            'id': 'timestamp',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Current timestamp'
                        }
                    ],
                    'status': {
                        'disabledReason': 'Vertices already exist.',
                        'enabled': false
                    },
                    'title': 'Metagraph',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'New-vertex-set',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '10',
                            'id': 'size',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Vertex set size'
                        }
                    ],
                    'status': {
                        'disabledReason': 'Vertices already exist.',
                        'enabled': false
                    },
                    'title': 'New vertex set',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Pull-segmentation-one-level-up',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Pull segmentation one level up',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Reverse-edge-direction',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Reverse edge direction',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Sample-edges-from-co-occurrence',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': 'This operation is only available for segmentations.',
                        'enabled': false
                    },
                    'title': 'Sample edges from co-occurrence',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Segment-by-geographical-proximity',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '',
                            'id': 'name',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Name'
                        },
                        {
                            'defaultValue': 'location',
                            'id': 'position',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'location',
                                    'title': 'location'
                                }
                            ],
                            'title': 'Position'
                        },
                        {
                            'defaultValue': '',
                            'id': 'shapefile',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Shapefile'
                        },
                        {
                            'defaultValue': '0.0',
                            'id': 'distance',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Distance'
                        },
                        {
                            'defaultValue': 'true',
                            'id': 'onlyknownFeatures',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'true',
                                    'title': 'true'
                                },
                                {
                                    'id': 'false',
                                    'title': 'false'
                                }
                            ],
                            'title': 'Only allow known features'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Segment by geographical proximity',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Split-edges',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'weight',
                            'id': 'rep',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'weight',
                                    'title': 'weight'
                                }
                            ],
                            'title': 'Repetition attribute'
                        },
                        {
                            'defaultValue': 'index',
                            'id': 'idx',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Index attribute name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Split edges',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Split-vertices',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': 'age',
                            'id': 'rep',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': 'age',
                                    'title': 'age'
                                },
                                {
                                    'id': 'income',
                                    'title': 'income'
                                },
                                {
                                    'id': 'page_rank',
                                    'title': 'page_rank'
                                }
                            ],
                            'title': 'Repetition attribute'
                        },
                        {
                            'defaultValue': 'new_id',
                            'id': 'idattr',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'ID attribute name'
                        },
                        {
                            'defaultValue': 'index',
                            'id': 'idx',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'Index attribute name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Split vertices',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Triadic-closure',
                    'isWorkflow': false,
                    'parameters': [],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Triadic closure',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                },
                {
                    'category': 'Structure operations',
                    'description': '',
                    'id': 'Union-with-another-project',
                    'isWorkflow': false,
                    'parameters': [
                        {
                            'defaultValue': '!checkpoint(1487334221263,meta)',
                            'id': 'other',
                            'kind': 'choice',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [
                                {
                                    'id': '!checkpoint(1487334221263,meta)',
                                    'title': 'meta (2017-02-17 13:23 CET)'
                                },
                                {
                                    'id': '!checkpoint(1487333438983,x123)',
                                    'title': 'x123 (2017-02-17 13:10 CET)'
                                }
                            ],
                            'title': 'Other project\'s name'
                        },
                        {
                            'defaultValue': 'new_id',
                            'id': 'id-attr',
                            'kind': 'default',
                            'mandatory': true,
                            'multipleChoice': false,
                            'options': [],
                            'title': 'ID attribute name'
                        }
                    ],
                    'status': {
                        'disabledReason': '',
                        'enabled': true
                    },
                    'title': 'Union with another project',
                    'visibleScalars': [],
                    'workflowAuthor': ''
                }
            ],
            'title': 'Structure operations'
        }
    ];

});


