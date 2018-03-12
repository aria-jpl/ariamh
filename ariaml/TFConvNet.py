#!/usr/bin/env python2.7
"""
Created on Mon Feb 15 16:07:23 2016

@author: giangi
"""
import tensorflow as tf
import re
import cPickle as cp
import numpy as np
import sys
import json
import os
from copy import deepcopy
class Data(object):
    def __init__(self,data,num_classes,batch_size):
        self._data = data[0]
        self._labels = data[1]
        self._indx_orig = data[2]
        self._start = 0
        self._batch_size = batch_size
        self._num_classes = num_classes

    def dump(self,filename):
        self._data.tofile(filename + '_features_' + '_'.join([str(i) for i in self._data.shape]) + '.fts')
        self._labels.tofile(filename + '_labels_' + '_'.join([str(i) for i in self._labels.shape]) + '.lab')
        self._indx_orig.tofile(filename + '_indx_' + '_'.join([str(i) for i in self._indx_orig.shape]) + '.ind')

               
    def _next(self,one_hot = False):
        pos = self._start + self._batch_size 
        if  pos > len(self._data):
            perm = np.arange(self._data.shape[0])
            np.random.shuffle(perm)
            self._data = self._data[perm]
            self._labels = self._labels[perm]
            self._start = 0
            pos = self._start + self._batch_size 

        ret = self._data[self._start:pos],self.one_hot(self._labels[self._start:pos]) if one_hot else self._labels[self._start:pos] 
        self._start = pos  
        return ret
        
    def one_hot(self,labels):
        eye = np.eye(self._num_classes)
        return eye[labels,:]        
        
class TFConvNet(object):
    def __init__(self):
        self._data = {} 
        self._nlabs = None
        self._ini_std = [0.0001,0.04]
        self._ini_const = 0.1
        self._weight_decay = [0,0.004]
        self._tower_name = 'tower'
        self._summary = False
        self._num_examples_per_epoch_for_train = None
        self._num_examples_per_epoch_for_eval = None
        self._batch_size = 128
        self._decay_step = 1000
        self._initial_learning_rate = .1
        self._learning_rate_decay_factor = .99
        self._moving_average_decay = .9999
        self._dims = []
        self._dropout = True
        self._data_type = 'train'
        self._one_hot = False
        self._input_dir = ''
        self._filter_size = [5,5]
        self._ksize = [3,3]
        self._pool_stride = [2,2]
        self._norm_params = [4,1,.001/9.,0.75] # 4, bias=1.0, alpha=0.001 / 9.0, beta=0.75
        self._cpu = '0'
    #unfortunately Tensor and ndarray have different way to get the shape    
    def get_shape(self,tensor):
        ret = None
        if isinstance(tensor,np.ndarray):
            ret = tensor.shape
        elif isinstance(tensor,tf.Tensor):
            ret = tensor.get_shape().as_list()
        return ret
    def get_inputs(self):
        return self._data[self._data_type]._next(self._one_hot)
        
    def inputs(self,filename):
        inps = json.load(open(os.path.join(self._input_dir,filename)))
        for k,v in inps.items():
            self._data[k] = Data(os.path.join(self._input_dir,v),self._nlabs,self._batch_size,self._input_dir)
        
    def _activation_summary(self,x):
        """Helper to create summaries for activations.
        Creates a summary that provides a histogram of activations.
        Creates a summary that measure the sparsity of activations.
        Args:
          x: Tensor
        Returns:
          nothing
        """
        # Remove 'tower_[0-9]/' from the name in case this is a multi-GPU training
        # session. This helps the clarity of presentation on tensorboard.
        tensor_name = re.sub('%s_[0-9]*/' % self._tower_name, '', x.op.name)
        tf.histogram_summary(tensor_name + '/activations', x)
        tf.scalar_summary(tensor_name + '/sparsity', tf.nn.zero_fraction(x))
        
    def _variable_on_cpu(self,name, shape, initializer):
        """Helper to create a Variable stored on CPU memory.
        Args:
          name: name of the variable
          shape: list of ints
          initializer: initializer for Variable
        Returns:
          Variable Tensor
        """
        with tf.device('/cpu:' + self._cpu):
          var = tf.get_variable(name, shape, initializer=initializer)
        return var
    def _variable_with_weight_decay(self,name, shape, stddev, wd):
        """Helper to create an initialized Variable with weight decay.
        Note that the Variable is initialized with a truncated normal distribution.
        A weight decay is added only if one is specified.
        Args:
          name: name of the variable
          shape: list of ints
          stddev: standard deviation of a truncated Gaussian
          wd: add L2Loss weight decay multiplied by this float. If None, weight
              decay is not added for this Variable.
        Returns:
          Variable Tensor
        """
        var = self._variable_on_cpu(name, shape,
                               tf.truncated_normal_initializer(stddev=stddev))
        if wd:
            weight_decay = tf.mul(tf.nn.l2_loss(var), wd, name='weight_loss')
            tf.add_to_collection('losses', weight_decay)
          
        return var

    def add_pool_norm(self,inps,name):
        ksize = [1,self._ksize[0],self._ksize[1],1]
        strides = [1,self._pool_stride[0],self._pool_stride[1],1]

        pool = tf.nn.max_pool(inps, ksize = ksize[:], strides=strides,
                         padding='SAME', name=name[0])
        # norm
        norm = tf.nn.lrn(pool, self._norm_params[0], bias=self._norm_params[1], alpha=self._norm_params[2], beta=self._norm_params[3],
                    name=name[1])
        return norm
    
    def add_conv_layer(self,inps,layer,scope,strides=None):
        shape = deepcopy(self._filter_size)
        shape.extend([self.get_shape(inps)[3],self._dims[layer]])
        if strides is None:
            strides = [1,1,1,1]
        kernel = self._variable_with_weight_decay('weights', shape=shape,
                                         stddev=self._ini_std[0], wd=self._weight_decay[0])
        conv = tf.nn.conv2d(inps, kernel, strides, padding='SAME')
        biases = self._variable_on_cpu('biases', [shape[3]], tf.constant_initializer(0.0))
        bias = tf.nn.bias_add(conv, biases)
        output = tf.nn.relu(bias, name=scope.name)
        if self._summary:
            self._activation_summary(output)
        return output
    
    def add_full_layer(self,inps,layer,scope):
        """
        Args:
            inps = batch_size x feature size input array
            layer = layer dim index
            scope = scope
        """
        l_dim = self._dims[layer]
        weights = self._variable_with_weight_decay('weights', shape=[self.get_shape(inps)[1], l_dim],
                                          stddev=self._ini_std[1], wd=self._weight_decay[1])
        biases = self._variable_on_cpu('biases', [l_dim], tf.constant_initializer(self._ini_const))
        output =  tf.nn.relu(tf.matmul(inps, weights) + biases, name=scope.name)
        if self._summary:
            self._activation_summary(output)
        return output
    
    def read_out(self,inps,scope):
        weights = self._variable_with_weight_decay('weights', [self.get_shape(inps)[1], self._nlabs],
                                          stddev=1/float(self.get_shape(inps)[1]), wd=0.0)
        biases = self._variable_on_cpu('biases', [self._nlabs],
                              tf.constant_initializer(0.0))
        softmax_linear = tf.add(tf.matmul(inps, weights), biases, name=scope.name)
        if self._summary:
            self._activation_summary(softmax_linear)

        return softmax_linear

    def loss(self,logits, labels):
        """Add L2Loss to all the trainable variables.
        Add summary for for "Loss" and "Loss/avg".
        Args:
          logits: Logits from inference().
          labels: Labels from distorted_inputs or inputs(). 1-D tensor
                  of shape [batch_size]
        Returns:
          Loss tensor of type float.
        """
        # Calculate the average cross entropy loss across the batch.
        sparse_labels = tf.reshape(labels, [self._batch_size, 1])
        indices = tf.reshape(tf.range(self._batch_size), [self._batch_size, 1])
        concated = tf.concat(1, [indices, sparse_labels])
        try:
            dense_labels = tf.sparse_to_dense(concated,
                                              [self._batch_size, self._nlabs],
                                              1.0, 0.0)
        except Exception:
            import pdb
            pdb.set_trace()
        # Calculate the average cross entropy loss across the batch.
        cross_entropy = tf.nn.softmax_cross_entropy_with_logits(
            logits, dense_labels, name='cross_entropy_per_example')
        cross_entropy_mean = tf.reduce_mean(cross_entropy, name='cross_entropy')
        tf.add_to_collection('losses', cross_entropy_mean)
        
        # The total loss is defined as the cross entropy loss plus all of the weight
        # decay terms (L2 loss).
        return tf.add_n(tf.get_collection('losses'), name='total_loss')

    def _add_loss_summaries(self,total_loss):
        """Add summaries for losses in  model.
        Generates moving average for all losses and associated summaries for
        visualizing the performance of the network.
        Args:
          total_loss: Total loss from loss().
        Returns:
          loss_averages_op: op for generating moving averages of losses.
        """
        # Compute the moving average of all individual losses and the total loss.
        loss_averages = tf.train.ExponentialMovingAverage(0.9, name='avg')
        losses = tf.get_collection('losses')
        loss_averages_op = loss_averages.apply(losses + [total_loss])
        
        # Attach a scalar summary to all individual losses and the total loss; do the
        # same for the averaged version of the losses.
        if self._summary:
            for l in losses + [total_loss]:
                # Name each loss as '(raw)' and name the moving average version of the loss
                # as the original loss name.
                tf.scalar_summary(l.op.name +' (raw)', l)
                tf.scalar_summary(l.op.name, loss_averages.average(l))
            
        return loss_averages_op

    
    def evaluation(self,logits, labels,prob=None):
        """Evaluate the quality of the logits at predicting the label.
    
        Args:
            logits: Logits tensor, float - [batch_size, NUM_CLASSES].
            labels: Labels tensor, int32 - [batch_size], with values in the
            range [0, NUM_CLASSES).
            probs: if None it consider corrects the class with higher probability
                   otherwise it considers labels 0 correct if the corresponding 
                   probability is greater than prob. Only works with binary classification  
        Returns:
              A int32 tensor with 1 in the positions correctly predicted.
        """
        if prob is None:
            # For a classifier model, we can use the in_top_k Op.
            # It returns a bool tensor with shape [batch_size] that is true for
            # the examples where the label's is was in the top k (here k=1)
            # of all logits for that example.
            correct = tf.nn.in_top_k(logits, labels, 1)
            # Return the number of true entries.
            ret = tf.cast(correct, tf.int32)
        else:
            
            nlogits = tf.nn.softmax(logits)
            #zeros = tf.slice(nlogits,[0,0],[labels.get_shape().as_list()[0],1])  
            ones = tf.squeeze(tf.slice(nlogits,[0,1],[labels.get_shape().as_list()[0],1]))         
            tocmp = tf.ones((labels.get_shape().as_list()[0],))
            tocmp = tf.scalar_mul(1 - prob,tocmp)
            #compare 1 - prob with the label ones since it's faster because no need to take the not
            #of the results
            onescmp = tf.greater_equal(ones, tocmp)
            predict = tf.cast(onescmp,tf.int32)
            #the ones in onescmp are the predicted 1 and the zeros are the predicted 0.
            #the correct predictions are the places where the labels are equal to onescmp
            ret = tf.cast(tf.equal(predict,labels),tf.int32)
        return ret
         
    def train(self,total_loss,global_step):
        """Train  model.
        Create an optimizer and apply to all trainable variables. Add moving
        average for all trainable variables.
        Args:
          total_loss: Total loss from loss().
          global_step: Integer Variable counting the number of training steps
            processed.
        Returns:
          train_op: op for training.
        """
        # Variables that affect learning rate.
        '''
        # Decay the learning rate exponentially based on the number of steps.
        lr = tf.train.exponential_decay(self._initial_learning_rate,
                                        global_step,
                                        self._decay_step,
                                        self._learning_rate_decay_factor,
                                        staircase=True)
        if self._summary:   
            tf.scalar_summary('learning_rate', lr)
        '''
            # Generate moving averages of all losses and associated summaries.
        loss_averages_op = self._add_loss_summaries(total_loss)
        
        # Compute gradients.
        with tf.control_dependencies([loss_averages_op]):
            #opt = tf.train.GradientDescentOptimizer(lr)
            opt = tf.train.AdamOptimizer(self._initial_learning_rate)
            grads = opt.compute_gradients(total_loss)

        # Apply gradients.
        apply_gradient_op = opt.apply_gradients(grads, global_step=global_step)
        
        # Add histograms for trainable variables.
        for var in tf.trainable_variables():
            tf.histogram_summary(var.op.name, var)
        
        # Add histograms for gradients.
        for grad, var in grads:
            if grad:
                tf.histogram_summary(var.op.name + '/gradients', grad)
        
        # Track the moving averages of all trainable variables.
        variable_averages = tf.train.ExponentialMovingAverage(
            self._moving_average_decay, global_step)
        variables_averages_op = variable_averages.apply(tf.trainable_variables())
        
        with tf.control_dependencies([apply_gradient_op, variables_averages_op]):
            train_op = tf.no_op(name='train')
        
        return train_op


def main():
    dt = Data(sys.argv[1],10,15)
    a,b = dt._next()
    c,d = dt._next(True)

if __name__ == '__main__':
    sys.exit(main())
