package com.soteradefense.dga.graphx.louvain

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.Graph.graphToGraphOps
import scala.math.BigDecimal.double2bigDecimal



/**
 * Provides low level louvain community detection algorithm functions.  Generally used by LouvainHarness
 * to coordinate the correct execution of the algorithm though its several stages.
 * 
 * For details on the sequential algorithm see:  Fast unfolding of communities in large networks, Blondel 2008
 */
object LouvainCore {

  
  
   /**
    * Generates a new graph of type Graph[VertexState,Long] based on an input graph of type.
    * Graph[VD,Long].  The resulting graph can be used for louvain computation.
    * 
    */
   /**
     * 初始化louvain的图
     * nodeWeight：计算节点对应的边的权重和，作为节点的权重
     * community：节点的id作为节点的初始社团id
     * communitySigmaTot：与社区内节点相连的边的权重和，初始化为节点的权重
     * internalWeight：社区内部边的权重和，最初社区只有一个节点无内部边，初始化为0
     */
   def createLouvainGraph[VD: ClassTag](graph: Graph[VD,Long]) : Graph[VertexState,Long]= {
    // Create the initial Louvain graph.  
    val nodeWeightMapFunc = (e:EdgeTriplet[VD,Long]) => Iterator((e.srcId,e.attr), (e.dstId,e.attr))
    val nodeWeightReduceFunc = (e1:Long,e2:Long) => e1+e2
    val nodeWeights = graph.mapReduceTriplets(nodeWeightMapFunc,nodeWeightReduceFunc)
    
    val louvainGraph = graph.outerJoinVertices(nodeWeights)((vid,data,weightOption)=> { 
      val weight = weightOption.getOrElse(0L)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = weight
      state.internalWeight = 0L
      state.nodeWeight = weight
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
    
    return louvainGraph
   }
  
    
   
  /**
   * Transform a graph from [VD,Long] to a a [VertexState,Long] graph and label each vertex with a community
   * to maximize global modularity (without compressing the graph)
   */
  def louvainFromStandardGraph[VD: ClassTag](sc:SparkContext,graph:Graph[VD,Long], minProgress:Int=1,progressCounter:Int=1) : (Double,Graph[VertexState,Long],Int) = {
	  val louvainGraph = createLouvainGraph(graph)
	  return louvain(sc,louvainGraph,minProgress,progressCounter)
  }
  
  
  
  /**
   * For a graph of type Graph[VertexState,Long] label each vertex with a community to maximize global modularity. 
   * (without compressing the graph)
   */
    /**
      *  louvain的核心算法
      *  输入：louvain图
      *  输出：输出图的模块度及带社区标签的louvian图
      */
  def louvain(sc:SparkContext, graph:Graph[VertexState,Long], minProgress:Int=1,progressCounter:Int=1) : (Double,Graph[VertexState,Long],Int)= {
    var louvainGraph = graph.cache()
    val graphWeight = louvainGraph.vertices.values.map(vdata=> vdata.internalWeight+vdata.nodeWeight).reduce(_+_)
      // 图的权重和，及全图的边权重的和，即公式中的M
    var totalGraphWeight = sc.broadcast(graphWeight) 
    println("totalEdgeWeight: "+totalGraphWeight.value)
    
    // gather community information from each vertex's local neighborhood
      //计算节点的邻居的信息
    var msgRDD = louvainGraph.mapReduceTriplets(sendMsg,mergeMsg)
    var activeMessages = msgRDD.count() //materializes the msgRDD and caches it in memory
     
    var updated = 0L - minProgress
    var even = false  
    var count = 0
    val maxIter = 100000 
    var stop = 0
    var updatedLastPhase = 0L
    do { 
       count += 1
        //控制奇偶轮数
	   even = ! even	   
	  
	   // label each vertex with its best community based on neighboring community information
        // 对节点进行社区划分，为每个节点分配一个合适的社区，获得一个包含了更新了社区信息后的节点集合
	   val labeledVerts = louvainVertJoin(louvainGraph,msgRDD,totalGraphWeight,even).cache()   
	   
	   // calculate new sigma total value for each community (total weight of each community)
        // 统计进行过一轮迭代后，图中节点的社区信息
        // 社区的sigma total = 社区的节点的权重 +  社区内部的边的权重
	   val communtiyUpdate = labeledVerts
	    .map( {case (vid,vdata) => (vdata.community,vdata.nodeWeight+vdata.internalWeight)})
	   .reduceByKey(_+_).cache()
	   
	   // map each vertex ID to its updated community information
        // 统计社区的映射信息，得到节点对应的社区id和社区sigmatot
	   val communityMapping = labeledVerts
	     .map( {case (vid,vdata) => (vdata.community,vid)})
	     .join(communtiyUpdate)
	     .map({case (community,(vid,sigmaTot)) => (vid,(community,sigmaTot)) })
	   .cache()
	   
	   // join the community labeled vertices with the updated community info
        // 更新整个图的节点的节点信息
	   val updatedVerts = labeledVerts.join(communityMapping).map({ case (vid,(vdata,communityTuple) ) => 
	     vdata.community = communityTuple._1  
	     vdata.communitySigmaTot = communityTuple._2
	     (vid,vdata)
	   }).cache()
	   updatedVerts.count()
	   labeledVerts.unpersist(blocking = false)
	   communtiyUpdate.unpersist(blocking=false)
	   communityMapping.unpersist(blocking=false)

        //保留更新前的图
	   val prevG = louvainGraph
        //更新图的节点信息
	   louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
	   louvainGraph.cache()
	   
       // gather community information from each vertex's local neighborhood
        // 保留更新前的邻居信息数据
	   val oldMsgs = msgRDD
        //使用新图重新计算邻居信息
       msgRDD = louvainGraph.mapReduceTriplets(sendMsg, mergeMsg).cache()
       activeMessages = msgRDD.count()  // materializes the graph by forcing computation
	 
       oldMsgs.unpersist(blocking=false)
       updatedVerts.unpersist(blocking=false)
       prevG.unpersistVertices(blocking=false)
       
       // half of the communites can swtich on even cycles
       // and the other half on odd cycles (to prevent deadlocks)
       // so we only want to look for progess on odd cycles (after all vertcies have had a chance to move)
        // 由于通过奇偶轮数来控制更新的频次，在偶数轮更新一般，在奇数轮更新另一半，因此在奇数轮更新完毕
	   if (even) updated = 0
	   updated = updated + louvainGraph.vertices.filter(_._2.changed).count 
	   if (!even) {
	     println("  # vertices moved: "+java.text.NumberFormat.getInstance().format(updated))
	     if (updated >= updatedLastPhase - minProgress) stop += 1
	     updatedLastPhase = updated
	   }

   
    } while ( stop <= progressCounter && (even ||   (updated > 0 && count < maxIter)))
    println("\nCompleted in "+count+" cycles")
      // 在偶数轮判定是否停止迭代，如果当前轮修改的节点个数比上一轮没有明显的减少（难以收敛或者可能有震荡），在允许的次数范围内进行迭代，这样的情况超过一定次数就停止迭代
      // 在偶数轮如果没有节点发生归属社区发生变化，那么就是理想的收敛状态
      // 未满足上述两种情况是，当迭代次数达到最大阈值也停止迭代
   
   
    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val newVerts = louvainGraph.vertices.innerJoin(msgRDD)((vid,vdata,msgs)=> {
        // 对于迭代结束的图，
        // sum the nodes internal weight and all of its edges that are in its community
        val community = vdata.community
        var k_i_in = vdata.internalWeight
        var sigmaTot = vdata.communitySigmaTot.toDouble
        // 通过这一步计算当前节点的邻居中与自己是同一个社区的节点之间的连边的权重，作为节点的k_i_in
        msgs.foreach({ case( (communityId,sigmaTotal),communityEdgeWeight ) => 
          if (vdata.community == communityId) k_i_in += communityEdgeWeight})
        val M = totalGraphWeight.value
        val k_i = vdata.nodeWeight + vdata.internalWeight
        //这个地方是deltaQ的公式
        var q = (k_i_in.toDouble / M) -  ( ( sigmaTot *k_i) / math.pow(M, 2) )
        //println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
        if (q < 0) 0 else q
    })
      //可以参考deltaQ的公式与最终模块度的公式，具有这种累加和的关系
    val actualQ = newVerts.values.reduce(_+_)
    
    // return the modularity value of the graph along with the 
    // graph. vertices are labeled with their community
    return (actualQ,louvainGraph,count/2)
   
  }
  

  /**
   * Creates the messages passed between each vertex to convey neighborhood community data.
   */
    /**
      * 根据边信息，统计节点对应的邻居信息的社区id，社区sigmatot，并保留边权重
      */
  private def sendMsg(et:EdgeTriplet[VertexState,Long]) = {
    val m1 = (et.dstId,Map((et.srcAttr.community,et.srcAttr.communitySigmaTot)->et.attr))
	val m2 = (et.srcId,Map((et.dstAttr.community,et.dstAttr.communitySigmaTot)->et.attr))
	Iterator(m1, m2)    
  }
  
  
  
  /**
   *  Merge neighborhood community data into a single message for each vertex
    *  将邻居信息进行合并，统计邻居社区对应的边权重和
   */
  private def mergeMsg(m1:Map[(Long,Long),Long],m2:Map[(Long,Long),Long]) ={
    val newMap = scala.collection.mutable.HashMap[(Long,Long),Long]()
    m1.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }
  
  
  
   /**
   * Join vertices with community data form their neighborhood and select the best community for each vertex to maximize change in modularity.
   * Returns a new set of vertices with the updated vertex state.
     * 根据邻居节点的信息，对图进行迭代，选出一个使模块度增益最大的社区划分结果
     * 返回一个新的节点集合，更新其中节点对应的社区的信息
   */
  private def louvainVertJoin(louvainGraph:Graph[VertexState,Long], msgRDD:VertexRDD[Map[(Long,Long),Long]], totalEdgeWeight:Broadcast[Long], even:Boolean) = {
     louvainGraph.vertices.innerJoin(msgRDD)( (vid, vdata, msgs)=> {
         //初始化图中节点的best_communityid为当前的社区id
	      var bestCommunity = vdata.community
		  var startingCommunityId = bestCommunity
		  var maxDeltaQ = BigDecimal(0.0);
	      var bestSigmaTot = 0L

         //遍历节点的邻居数据，统计如果将节点纳入到社区内，模块度的增益也就是公式中的deltaQ，取最大的deltaQ和对应的社区id和社区的sigmatot
	      msgs.foreach({ case( (communityId,sigmaTotal),communityEdgeWeight ) => 
	      	val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, vdata.nodeWeight, vdata.internalWeight,totalEdgeWeight.value)
	        //println("   communtiy: "+communityId+" sigma:"+sigmaTotal+" edgeweight:"+communityEdgeWeight+"  q:"+deltaQ)
	        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))){
	          maxDeltaQ = deltaQ
	          bestCommunity = communityId
	          bestSigmaTot = sigmaTotal
	        }
	      })	      
	      // only allow changes from low to high communties on even cyces and high to low on odd cycles
         //通过奇偶轮数，控制节点更新，防止震荡
		  if ( vdata.community != bestCommunity && ( (even && vdata.community > bestCommunity)  || (!even && vdata.community < bestCommunity)  )  ){
		    //println("  "+vid+" SWITCHED from "+vdata.community+" to "+bestCommunity)
		    vdata.community = bestCommunity
		    vdata.communitySigmaTot = bestSigmaTot  
		    vdata.changed = true
		  }
		  else{
		    vdata.changed = false
		  }   
	     vdata
	   })
  }
  
  
  
  /**
   * Returns the change in modularity that would result from a vertex moving to a specified community.
   */
  private def q(currCommunityId:Long, testCommunityId:Long, testSigmaTot:Long, edgeWeightInCommunity:Long, nodeWeight:Long, internalWeight:Long, totalEdgeWeight:Long) : BigDecimal = {
	  	val isCurrentCommunity = (currCommunityId.equals(testCommunityId));
		val M = BigDecimal(totalEdgeWeight); 
	  	val k_i_in_L =  if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity;
		val k_i_in = BigDecimal(k_i_in_L);
		val k_i = BigDecimal(nodeWeight + internalWeight);
		val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot);
		
		var deltaQ =  BigDecimal(0.0);
		if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
			deltaQ = k_i_in - ( k_i * sigma_tot / M)
			//println(s"      $deltaQ = $k_i_in - ( $k_i * $sigma_tot / $M")		
		}
		return deltaQ;
  }
  
  
  
  /**
   * Compress a graph by its communities, aggregate both internal node weights and edge
   * weights within communities.
    * 对社区进行压缩
   */
  def compressGraph(graph:Graph[VertexState,Long],debug:Boolean=true) : Graph[VertexState,Long] = {

    // aggregate the edge weights of self loops. edges with both src and dst in the same community.
	// WARNING  can not use graph.mapReduceTriplets because we are mapping to new vertexIds
      // 计算不同社区的内部边权重
    val internalEdgeWeights = graph.triplets.flatMap(et=>{
    	if (et.srcAttr.community == et.dstAttr.community){
            Iterator( ( et.srcAttr.community, 2*et.attr) )  // count the weight from both nodes  // count the weight from both nodes
          } 
          else Iterator.empty  
    }).reduceByKey(_+_)
    
     
    // aggregate the internal weights of all nodes in each community
      // 社区内所有节点的interalWeight的和， 第一轮迭代结束时是0
      // 第一次压缩时，节点的internalWeight是压缩的节点的那个社区内的边权重和
      // 那么第二次压缩时，节点的internalWeight会包含当前社区的内部边权重和及节点的环权重（即上一轮压缩成节点的社区的内部权重和）
      // 以此类推
    var internalWeights = graph.vertices.values.map(vdata=> (vdata.community,vdata.internalWeight)).reduceByKey(_+_)

      // 更新压缩节点的信息
    // join internal weights and self edges to find new interal weight of each community
    val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({case (vid,(weight1,weight2Option)) =>
      val weight2 = weight2Option.getOrElse(0L)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = 0L
      state.internalWeight = weight1+weight2
      state.nodeWeight = 0L
      (vid,state)
    }).cache()
    
    // 更新压缩节点之间的边信息
    // translate each vertex edge to a community edge
    val edges = graph.triplets.flatMap(et=> {
       val src = math.min(et.srcAttr.community,et.dstAttr.community)
       val dst = math.max(et.srcAttr.community,et.dstAttr.community)
       if (src != dst) Iterator(new Edge(src, dst, et.attr))
       else Iterator.empty
    }).cache()
    
    
    // generate a new graph where each community of the previous
    // graph is now represented as a single vertex
    val compressedGraph = Graph(newVerts,edges)
      .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
    
    // calculate the weighted degree of each node
    val nodeWeightMapFunc = (e:EdgeTriplet[VertexState,Long]) => Iterator((e.srcId,e.attr), (e.dstId,e.attr))
    val nodeWeightReduceFunc = (e1:Long,e2:Long) => e1+e2
    val nodeWeights = compressedGraph.mapReduceTriplets(nodeWeightMapFunc,nodeWeightReduceFunc)
    
    // fill in the weighted degree of each node
   // val louvainGraph = compressedGraph.joinVertices(nodeWeights)((vid,data,weight)=> { 
   val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid,data,weightOption)=> { 
      val weight = weightOption.getOrElse(0L)
      data.communitySigmaTot = weight +data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count() // materialize the graph
    
    newVerts.unpersist(blocking=false)
    edges.unpersist(blocking=false)
    return louvainGraph
    
   
    
  }
  

  
 
   // debug printing
   private def printlouvain(graph:Graph[VertexState,Long]) = {
     print("\ncommunity label snapshot\n(vid,community,sigmaTot)\n")
     graph.vertices.mapValues((vid,vdata)=> (vdata.community,vdata.communitySigmaTot)).collect().foreach(f=>println(" "+f))
   }
  
 
   
   // debug printing
   private def printedgetriplets(graph:Graph[VertexState,Long]) = {
     print("\ncommunity label snapshot FROM TRIPLETS\n(vid,community,sigmaTot)\n")
     (graph.triplets.flatMap(e=> Iterator((e.srcId,e.srcAttr.community,e.srcAttr.communitySigmaTot), (e.dstId,e.dstAttr.community,e.dstAttr.communitySigmaTot))).collect()).foreach(f=>println(" "+f))
   }
  
 
   
}