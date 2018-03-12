import os
import sys
import json
from math import  floor, ceil
import numpy as np
import matplotlib
from matplotlib import pyplot as plt
import pickle
#from sklearn.externals.joblib import load as jlload


class Predictor:
    def __init__(self, clf_json):
        """
        Parses a classifier specification .json which provides the path to a
        pre-trained .pickle classifier file, along with metadata specifying:
          - which coherence threshold to use
          - which feature groups to use and their order
          - dimensionality of each feature group
          - (any other details necessary to apply the classifier)
        
        Keyword Arguments:
        None
        
        Returns:
        parsed classifier + metadata
        """
        
        with open(clf_json,'r') as fid:
            clf_inputs = json.load(fid)
        with open(clf_inputs['feat_file'],'r') as fid:
            clf_inputsf = json.load(fid) 
        self.clf_file        = clf_inputs['clf_file']
        self.feature_order   = clf_inputsf['feature_order']
        self.feature_dims    = clf_inputsf['feature_dims']
        self.valid_cohthr10  = clf_inputsf['valid_cohthr10']
        self.cohthr10        = clf_inputsf['cohthr10']
        
        with open(self.clf_file,'rb') as fid:
            self.clf = pickle.load(fid)
            
        assert(self.clf.n_features_ == sum(self.feature_dims))

    def _parse_feats(self,feat_json,**kwargs):
        """
        _parse_feats(self,feat_json,**kwargs): parses a json file containing
        a set of thresholded features for a single interferogram
        
        Arguments:
        - feat_json: input json file
        
        Keyword Arguments:
        - cohthr10: coherence threshold to use (defaults to classifier cohthr10)
        
        Returns:
        list of extracted features at the specified threshold for the given interferogram
        """
        
        cohthr10 = kwargs.pop('cohthr10',self.cohthr10)
        assert(cohthr10 in self.valid_cohthr10)
        
        with open(feat_json,'r') as fid:
            feat_inputs = json.load(fid)

        assert(str(cohthr10) in feat_inputs)
        feat_thr = feat_inputs[str(cohthr10)]

        assert(all(feat_id in feat_thr for feat_id in self.feature_order))
        feat_list = []
        for feat_id in self.feature_order:
            flist = feat_thr[feat_id]
            if not isinstance(flist,list):
                flist = [flist]
            feat_list.extend(flist)
                        
        return feat_list

    def predict(self,inputs_json,**kwargs):
        """
        predict(self,inputs_json,**kwargs): generates an array of output predictions for one
        or more inteferograms, each represented as a feature*.json file. 
       
        Arguments:
        - inputs_json: either a single string or list of strings specifing the path to
        the feature.json file for each interferogram we wish to classify
        
        Keyword Arguments:
        - pred_kw: dict of keywords pass to classifier predict function (default={})
        
        Returns:
        - array of prediction probabilities for each input
        """
        pred_kw = kwargs.pop('pred_kw',{})
        if not isinstance(inputs_json,list):
            inputs_json = [inputs_json]

        feats = [self._parse_feats(jsoni,**kwargs) for jsoni in inputs_json]
        feats = np.atleast_2d(feats)
        assert(feats.shape[1] == self.clf.n_features_)

        # return probability of error (class 1)
        return [[self.clf.predict_proba(np.atleast_2d(featvec),**pred_kw)[0][1],int(self.clf.predict(np.atleast_2d(featvec))[0]) if self.clf.predict(np.atleast_2d(featvec))[0] == 1 else -1]
                for featvec in feats]
  

    
