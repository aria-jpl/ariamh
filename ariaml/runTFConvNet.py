#!/usr/bin/env python2.7
"""
Created on Mon Feb 15 16:07:23 2016

@author: giangi
"""
from __future__ import print_function
import tensorflow as tf
import argparse
from ariaml.TFConvNet import TFConvNet as RN, Data
import cPickle as cp
import numpy as np
import time
import json
from datetime import datetime
import os
import sys
import shutil
def parse(args):
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)  
    parser.add_argument('-a','--train_dir', type = str, default ='/tmp/train_dir' , dest = 'train_dir', help = 'training directory')
    parser.add_argument('-b', '--batch_size', type = int, default = 128, dest = 'batch_size', help = 'batch size')    
    parser.add_argument('-c','--const', type = float, default = .1, dest = 'const', help = 'initial value for bias')
    parser.add_argument('-d', '--n_decay', type = int, default = 1000, dest = 'n_decay', help = 'number epochs per decays')    
    parser.add_argument('-e', '--n_eval', type = int, default = 1000, dest = 'n_eval', help = 'number of example per epoch eval')    
    parser.add_argument('-f', '--feats_shape', type = int, required = True, nargs = '+', dest = 'feats_shape', help = 'size of the strides in pooling')    
    parser.add_argument('-g', '--norm_params', type = float, default = [4,1,.001/9.,0.75], nargs = '+', dest = 'norm_params', help = 'normalization parameters')    
    parser.add_argument('-i', '--input', type = str, required = True, dest = 'input', help = 'input features filename')    
    parser.add_argument('-j', '--filter_size', type = int, default = [5,5], nargs = '+', dest = 'filter_size', help = 'size of the conv filter')    
    parser.add_argument('-k', '--ksize', type = int, default = [3,3], nargs = '+', dest = 'ksize', help = 'size of the pooling') 
    parser.add_argument('-l','--learn_rate_init', type = float, default = .1, dest = 'learn_rate_init', help = 'initial learning rate')
    parser.add_argument('-m','--ma_decay', type = float, default = .1, dest = 'ma_decay', help = 'moving average decay')
    parser.add_argument('-n','--input_dir', type = str, default ='./' , dest = 'input_dir', help = 'input directory')
    parser.add_argument('-o','--output', type = str, default ='results' , dest = 'output', help = 'outpute file')
    parser.add_argument('-p', '--dropout', action = 'store_true', dest = 'dropout', help = 'apply dropout')    
    parser.add_argument('-q', '--pool_stride', type = int, default = [2,2], nargs = '+', dest = 'pool_stride', help = 'size of the strides in pooling')    
    parser.add_argument('-r','--learn_rate_dec', type = float, default = .99, dest = 'learn_rate_dec', help = 'learning rate decay factor')
    parser.add_argument('-s','--std', type = float, default = [.0001,.04], nargs = '+', dest = 'std', help = 'initial std value for weights for conv and connected layers')
    parser.add_argument('-t', '--n_train', type = int, default = 5000, dest = 'n_train', help = 'number of example per epoch train')    
    parser.add_argument('-u', '--summary', action = 'store_true', dest = 'summary', help = 'save summary')    
    parser.add_argument('-v', '--dims', type = int, default = [32,32,128,32], nargs = '+', dest = 'dims', help = 'layers dimensions')    
    parser.add_argument('-w','--weight_decay', type = float, default = [0,0.004], nargs = '+',dest = 'weight_decay', help = 'weight decay for conv and connected layers')
    parser.add_argument('-x', '--max_steps', type = int, default = 100000, dest = 'max_steps', help = 'max number of steps')    
    parser.add_argument('-y', '--nlabs', type = int, required = True, dest = 'nlabs', help = 'number of labels or classes')      
    parser.add_argument('-z', '--relabel', type = int, default = [], nargs = '+', dest = 'relabel', help = 'relabel the corresponding label')    
    parser.add_argument('--action', type = str, default ='train' , dest = 'action', help = 'action to perform')
    parser.add_argument('--clean', action = 'store_true', dest = 'clean', help = 'remove train dir')    
    parser.add_argument('--cpu', type = str, default ='0' , dest = 'cpu', help = 'which cpu using for processing')    
    parser.add_argument('--determ', action = 'store_true', dest = 'determ', help = 'make shuffle deterministic. Used for leave one out')    
    parser.add_argument('--dprob', type = float, default = .5, dest = 'dprob', help = 'dropout probability')
    parser.add_argument('--error', type = float, default = .05, dest = 'error', help = 'error value to exit')
    parser.add_argument('--gpu_mem', type = float, default = .5, dest = 'gpu_mem', help = 'percent gpu mem allocation')    
    parser.add_argument('--gpu', type = str, default ='0' , dest = 'gpu', help = 'which gpu using for processing')
    parser.add_argument('--l1o', type = int, default = 0 , dest = 'l1o', help = 'index of the one to leave out')
    parser.add_argument('--pid', type = str, default ='1' , dest = 'pid', help = 'process id to create separate training dir')  
    parser.add_argument('--prob', type = float, default = .5, dest = 'prob', help = 'for binary classification the threshold in the softmax for label 0')
    parser.add_argument('--remove', type = int, default = None, nargs = '+', dest = 'remove', help = 'Channel to remove in an image')    
    parser.add_argument('--max_label', type = int, default = 3 , dest = 'max_label', help = 'Maximun value of the labels. Remove everything above it')
    parser.add_argument('--print_rate', type = int, default = 100 , dest = 'print_rate', help = 'Number of steps to print the loss')
    parser.add_argument('--check_rate', type = int, default = 1000 , dest = 'check_rate', help = 'Number of steps to check the accuracy')

    inps = parser.parse_args(args) 
    inps.train_dir = inps.train_dir + '_' +  inps.pid
    return  inps  

def _init(args):
    rn = RN()
    rn._ini_std  = args.std
    rn._ini_const = args.const
    rn._weight_decay = args.weight_decay
    rn._summary = args.summary
    rn._num_examples_per_epoch_for_train = args.n_train
    rn._num_examples_per_epoch_for_eval = args.n_eval
    rn._batch_size = args.batch_size
    rn._decay_step = args.n_decay
    rn._initial_learning_rate = args.learn_rate_init
    rn._learning_rate_decay_factor = args.learn_rate_dec
    rn._moving_average_decay = args.ma_decay
    rn._dims = args.dims
    rn._dropout = args.dropout
    rn._input_dir = args.input_dir
    rn._nlabs = args.nlabs
    rn._norm_params = args.norm_params
    rn._cpu = args.cpu
    return rn

def get_stats(nlabs,labels,predict):
    '''
    nlabs = number of labels
    labels = initial labels
    predict array with ones where the prediction was correct
    '''
    #correct and incorrect
    cor = np.nonzero(predict == 1)[0]
    incor = np.nonzero(predict == 0)[0]
    cor_labels = labels[cor]
    incor_labels = labels[incor]
    ret  = []
    for i in range(nlabs):#since they are related
        tp = len(np.nonzero(cor_labels == i)[0])
        fp = len(np.nonzero(incor_labels == i)[0])
        tn = len(np.nonzero(cor_labels != i)[0])
        fn = len(np.nonzero(incor_labels != i)[0])
        ret.append([tp,fp,tn,fn])
    return ret
def print_results(results,save=0,output='output'):#0 print, 1  save, 2 print and save
    max_acc = 0
    for i,res in enumerate(results):
        print(res)
        pre = np.sum(res[0])/np.sum(res[0:2]).astype(np.float)
        rec = np.sum(res[0])/np.sum(res[[0,3]]).astype(np.float)
        acc = np.sum(res[[0,2]])/np.sum(res).astype(np.float)
        if save == 0 or save == 2:            
            print('Label ' + str(i),':')
            print('Accuracy:',str(acc))
            print('Precision:',str(pre))
            print('Recall:',str(rec))
            print('F1:',str(2*(pre*rec)/(pre + rec)))
        if save == 1 or save == 2:
            with open(output,'a') as fp:
                fp.write('Label ' + str(i) + ' :\n')
                fp.write('Accuracy: ' + str(acc) + '\n')
                fp.write('Precision: ' + str(pre) + '\n')
                fp.write('Recall: ' + str(rec) + '\n')
                fp.write('F1: ' + str(2*(pre*rec)/(pre + rec)) + '\n')
        if acc > max_acc:
            max_acc = acc
    return max_acc

def evaluate(getter,sess,eval_correct,inputs_holder,labels_holder,keep_prob_holder,eval_type):
    cnt = 0
    getter._data_type = eval_type
    num_data = getter._data[eval_type]._labels.shape[0]
    steps_per_epoch = num_data // getter._batch_size
    tot_res = []
    nlabs = getter._nlabs
    for i in range(steps_per_epoch):
        feed_dict = fill_feed_dict(getter._data[eval_type],
                               inputs_holder,
                               labels_holder)
        feed_dict[keep_prob_holder] = 1.0

        labels = feed_dict[labels_holder]
        correct = sess.run(eval_correct,feed_dict=feed_dict)
        tot_res.append(get_stats(nlabs,labels,correct))
    #return also the last evaluation it is used for testing to get the
    #indeces of the mispredicted 
    return np.sum(tot_res,0),correct  

def placeholder_inputs(shape):
    """Generate placeholder variables to represent the the input tensors.
    
    These placeholders are used as inputs by the rest of the model building
    code and will be fed from the downloaded data in the .run() loop, below.
    
    Args:
      batch_size: The batch size will be baked into both placeholders.
    
    Returns:
      images_placeholder: Images placeholder.
      labels_placeholder: Labels placeholder.
    """
    # Note that the shapes of the placeholders match the shapes of the full
    # image and label tensors, except the first dimension is now batch_size
    # rather than the full size of the train or test data sets.
    images_placeholder = tf.placeholder(tf.float32, shape=shape)
    labels_placeholder = tf.placeholder(tf.int32, shape=shape[0])
    keep_prob = tf.placeholder("float")
    return images_placeholder, labels_placeholder,keep_prob
 
def fill_feed_dict(data_set, images_pl, labels_pl):
    """Fills the feed_dict for training the given step.
    
    A feed_dict takes the form of:
    feed_dict = {
        <placeholder>: <tensor of values to be passed for placeholder>,
        ....
    }
    
    Args:
      data_set: The set of images and labels, from input_data.read_data_sets()
      images_pl: The images placeholder, from placeholder_inputs().
      labels_pl: The labels placeholder, from placeholder_inputs().
    
    Returns:
      feed_dict: The feed dictionary mapping from placeholders to values.
    """
    # Create the feed_dict for the placeholders filled with the next
    # `batch size ` examples.
    images_feed, labels_feed = data_set._next()
    feed_dict = {
        images_pl: images_feed,
        labels_pl: labels_feed,
    }
    return feed_dict

def read(filename,input_dir,relabel,determ=True,args=None):
    to_remove = args.remove
    inps = json.load(open(filename))
    data = np.reshape(np.fromfile(os.path.join(input_dir,inps['data']),np.float32),inps['size'])
    labels = np.fromfile(os.path.join(input_dir,inps['labels']),np.int32)
    sel = np.isnan(data)
    data[sel] = 0
    tr = labels <= args.max_label
    labels = labels[tr]
    data = data[tr,:,:,:]
    if to_remove:
        indx = range(data.shape[-1])
        s_remove = sorted(to_remove)
        for i in s_remove[::-1]:
            indx.pop(i)
        data = data[:,:,:,indx]
        
    for i,j in enumerate(relabel):
        if i == j:
            continue
        indx = np.nonzero(labels == i)
        labels[indx] = j
    #make it deterministic for testing  
    if determ:
        np.random.seed(12345)
        
    indx = np.arange(len(labels))
    np.random.shuffle(indx)
    return data[indx,:],labels[indx],indx

def inputs(_self,filename,relabel,determ=True,leavei=None,args=None):
    to_remove = args.remove
    inps = json.load(open(os.path.join(_self._input_dir,filename)))
    for k,v in inps.items():
        if k not in ['train','test','valid']:
            continue
        data = read(v,_self._input_dir,relabel,determ,args)
        _self._data[k] = Data(data,_self._nlabs,_self._batch_size)
    if 'leave_k' in inps.keys():       
        data = _self._data['train']._data[-inps['leave_k']:,:],\
        _self._data['train']._labels[-inps['leave_k']:],\
        _self._data['train']._indx_orig[-inps['leave_k']:]
        dt = Data(data,_self._nlabs,_self._batch_size)
        _self._data['test'] = dt       
        _self._data['train']._data = _self._data['train']._data[:-inps['leave_k'],:]
        _self._data['train']._labels = _self._data['train']._labels[:-inps['leave_k']]
        _self._data['train']._indx_orig = _self._data['train']._indx_orig[:-inps['leave_k']]
    elif 'leave_one_out' in inps.keys():
        #in this case only read the last and replicate batch size
        rep = [_self._batch_size]
        rep.extend(np.ones(len(_self._data['train']._data[leavei,:].shape),np.int32))
        data = np.tile(_self._data['train']._data[leavei,:],rep),\
        np.tile(_self._data['train']._labels[leavei],_self._batch_size),\
        np.tile(_self._data['train']._indx_orig[leavei],_self._batch_size)
        dt = Data(data,_self._nlabs,_self._batch_size)
        indx = np.arange( _self._data['train']._data.shape[0])
        indx = np.delete(indx,leavei)
        _self._data['test'] = dt       
        _self._data['train']._data = _self._data['train']._data[indx,:]
        _self._data['train']._labels = _self._data['train']._labels[indx]
        _self._data['train']._indx_orig = _self._data['train']._indx_orig[indx]
         
def inference(_self,inputs,keep_prob):
    cnt = 0
    with tf.variable_scope('conv1') as scope:
        conv1 = _self.add_conv_layer(inputs,cnt,scope)
    norm1 = _self.add_pool_norm(conv1,['pool1','norm1'])
    cnt += 1    
    with tf.variable_scope('conv2') as scope:
        conv2 = _self.add_conv_layer(norm1,cnt,scope)
    norm2 = _self.add_pool_norm(conv2,['pool2','norm2'])
    cnt += 1
    dims = np.array(_self.get_shape(norm2),dtype=np.int)
    norm2_reshape = tf.reshape(norm2,[dims[0],np.prod(dims[1:])])
    
    with tf.variable_scope('layer1') as scope:
        layer1 = _self.add_full_layer(norm2_reshape,cnt, scope) 
    cnt += 1
    
    with tf.variable_scope('layer2') as scope:
        layer2 = _self.add_full_layer(layer1,cnt, scope) 
    if _self._dropout:
        print('applying dropout')
        layer3 = tf.nn.dropout(layer2, keep_prob)
    else:
        layer3 = layer2      
    with tf.variable_scope('softmax_linear') as scope:
        softmax_linear = _self.read_out(layer3, scope)
    return softmax_linear

def run_eval(getter,args):
    with tf.Graph().as_default():
        global_step = tf.Variable(0, trainable=False)
        shape = [args.batch_size]
        shape.extend(args.feats_shape)
        images, labels, keep_prob = placeholder_inputs(shape)        
        # Build a Graph that computes the logits predictions from the
        # inference model.
        logits = inference(getter,images,keep_prob)
        # Add the Op to compare the logits to the labels during evaluation.
        eval_correct = getter.evaluation(logits, labels,args.prob)
        variable_averages = tf.train.ExponentialMovingAverage(
            getter._moving_average_decay, global_step)
        variables_to_restore = variable_averages.variables_to_restore()
        saver = tf.train.Saver(variables_to_restore)         
        ev = eval_once(saver,eval_correct,getter,images,labels,keep_prob,args.train_dir)
        print_results(ev)
def eval_once(saver,eval_correct,getter,inputs_holder,labels_holder,keep_prob_holder,train_dir):
    
    with tf.Session() as sess:
        ckpt = tf.train.get_checkpoint_state(train_dir)
        if ckpt and ckpt.model_checkpoint_path:
            # Restores from checkpoint
            saver.restore(sess, ckpt.model_checkpoint_path)
        else:
            print('No checkpoint file found')
            return
        
        eval_type = 'train'
        getter._data_type = eval_type
        all_labels = getter._data[eval_type]._labels
        all_data = getter._data[eval_type]._data

        num_data = all_labels.shape[0]
        steps_per_epoch = num_data // getter._batch_size
        left = num_data % getter._batch_size
        tot_res = []
        nlabs = getter._nlabs
        for i in range(steps_per_epoch):
            data_now = all_data[i*getter._batch_size:(i+1)*getter._batch_size,:]
            labels_now = all_labels[i*getter._batch_size:(i+1)*getter._batch_size]
            feed_dict = {inputs_holder:data_now,
                         labels_holder:labels_now}
            feed_dict[keep_prob_holder] = 1.0
        
            correct = sess.run(eval_correct,feed_dict=feed_dict)
            tot_res.append(get_stats(nlabs,labels_now,correct))
        if left:
            i += 1
            data_now[:left] = all_data[i*getter._batch_size:,:]
            labels_now[:left] = all_labels[i*getter._batch_size:]
            correct = sess.run(eval_correct,feed_dict=feed_dict)
            tot_res.append(get_stats(nlabs,labels_now[:left],correct[:left]))
        return np.sum(tot_res,0)  
      
def main(inps):
    """Train CIFAR-10 for a number of steps."""
    ret = None
    args = parse(inps)
    rn = _init(args)
    if args.clean:
        try:
            shutil.rmtree(args.train_dir)
        except Exception:
            pass
    inputs(rn,args.input,args.relabel,args.determ,args.l1o,args)
    if args.action == 'train':
    
        with tf.Graph().as_default():
            if args.gpu == '-1':
                dev = '/cpu:0'
                no_gpu = True
            else:
                dev = '/gpu:' + args.gpu
                no_gpu = False
            print(dev)
            with tf.device(dev):
                global_step = tf.Variable(0, trainable=False)
                shape = [args.batch_size]
                shape.extend(args.feats_shape)
                images, labels, keep_prob = placeholder_inputs(shape)
                
                
                # Build a Graph that computes the logits predictions from the
                # inference model.
                logits = inference(rn,images,keep_prob)
                
                # Calculate loss.
                loss = rn.loss(logits, labels)
                
                # Build a Graph that trains the model with one batch of examples and
                # updates the model parameters.
                train_op = rn.train(loss, global_step)
                
                # Add the Op to compare the logits to the labels during evaluation.
                eval_correct = rn.evaluation(logits, labels,args.prob)
                           
                # Create a saver.
                saver = tf.train.Saver(tf.all_variables())
                
                # Build the summary operation based on the TF collection of Summaries.
                summary_op = tf.merge_all_summaries()
                
                # Build an initialization operation to run below.
                init = tf.initialize_all_variables()
                
                # Start running operations on the Graph.
                if no_gpu:
                    sess = tf.Session(config=tf.ConfigProto(allow_soft_placement=True, log_device_placement=False,
                           device_count={'GPU':0}))
                else:
                    gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=args.gpu_mem)
                    sess = tf.Session(config=tf.ConfigProto(allow_soft_placement=True, log_device_placement=False,
                           gpu_options=gpu_options))
                sess.run(init)
                
                # Start the queue runners.
                tf.train.start_queue_runners(sess=sess)
                    
                summary_writer = tf.train.SummaryWriter(args.train_dir,
                                                        graph_def=sess.graph_def)
                acc_count = 0
                for step in xrange(args.max_steps):
                    start_time = time.time()
                    # Fill a feed dictionary with the actual set of images and labels
                    # for this particular training step.
                    feed_dict = fill_feed_dict(rn._data['train'],
                                           images,
                                           labels)
                    feed_dict[keep_prob] = args.dprob
                    
                    _, loss_value = sess.run([train_op, loss], feed_dict=feed_dict)
                    duration = time.time() - start_time
                    
                    assert not np.isnan(loss_value), 'Model diverged with loss = NaN'
                    save = 0
                    end_loop = False
                    print_res = False 
                    if step % args.print_rate == 0:
                        num_examples_per_step = args.batch_size
                        examples_per_sec = num_examples_per_step / duration
                        sec_per_batch = float(duration)
                        
                        format_str = ('%s: step %d, loss = %.2f (%.1f examples/sec; %.3f '
                                      'sec/batch)')
                        print (format_str % (datetime.now(), step, loss_value,
                                             examples_per_sec, sec_per_batch))
                        #in case no test still break
                        if loss_value < args.error:
                            acc_count += 1
                            save = 2
                            print_res = True
                        summary_str = sess.run(summary_op, feed_dict=feed_dict)
                        summary_writer.add_summary(summary_str, step)
                        if acc_count >= 5:
                            rn._data['test'].dump(os.path.join(args.train_dir,'eval'))              
                            end_loop = True
        
                        # Save the model checkpoint periodically.
                    if step % args.check_rate == 0 or (step + 1) == args.max_steps or print_res:
                        checkpoint_path = os.path.join(args.train_dir, 'model.ckpt')
                        saver.save(sess, checkpoint_path, global_step=step)
                        print('Training Data Eval:')
                        results,_ = evaluate(rn,sess,eval_correct,images,labels,keep_prob,'train')
                        max_acc = print_results(results)
                        if max_acc > .999:
                            acc_count += 1
                            save = 2
                        
                        if 'test' in rn._data.keys():
                            print('Test Data Eval:')
                            results,corrects = evaluate(rn,sess,eval_correct,images,labels,keep_prob,'test')
                            print_results(results,save,os.path.join(args.train_dir,args.output))
                            ret = results, rn._data['test']._indx_orig[corrects == 0], rn._data['test']._indx_orig
                        if 'valid' in rn._data.keys():
                            print('Validation Data Eval:')
                            results,_ = evaluate(rn,sess,eval_correct,images,labels,keep_prob,'valid')
                            print_results(results)
                    if end_loop:
                        break
    elif args.action == 'eval':
        run_eval(rn,args)
        
    return ret
    
if __name__ == '__main__':
    #_init(parse(sys.argv[1:]))
    main(sys.argv[1:])
    #inps = '-z 0 1 1 1 -y 2 -f 30 40 8 -i conv_tf_l1o_inputs.json  -u -x 10000  -p --gpu 0 --dprob .5 -l 0.001 --clean --pid 1'
    #sys.exit(main(inps.split()))
