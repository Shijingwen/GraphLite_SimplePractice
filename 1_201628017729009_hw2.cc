/* 1, 201628017729009, Shi Jingwen */
/**
 * @file KcoreVertex.cc
 * @author  Jingwen,Shi
 * @version 0.1
 * 
 * @section DESCRIPTION
 * 
 * This file implements the Kcore algorithm using graphlite API.
 *
 */

#include <stdio.h>
#include <string.h>
#include <math.h>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) KCore##name
int k = 0;

/** Definition of my new vertex struct. */
typedef struct MyVertex {
    bool is_deleted;/**record if the vertex exist now */
    int current_degree;/**num be of active edge*/
} MyVertex;

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(MyVertex);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(float);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(bool);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        float weight = 0;
        int outdegree = 0;
        struct MyVertex newVertex;// value of vertex
        newVertex.is_deleted = false;
        newVertex.current_degree = 0; 

        const char *line= getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable
        sscanf(line, "%lld %lld %f", &from, &to, &weight);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable

            sscanf(line, "%lld %lld %f", &from, &to, &weight);
            if (last_vertex != from) {
                newVertex.current_degree = outdegree;
                addVertex(last_vertex, &newVertex, outdegree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        newVertex.current_degree = outdegree;
        addVertex(last_vertex, &newVertex, outdegree);
        //printf("finish ID = %lld,degree = %d",last_vertex, newVertex.current_degree);
    }
    
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        MyVertex value;
        char s[1024];

        for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {
            r_iter.getIdValue(vid, &value);
            if(value.is_deleted == false)
	    {
            	int n = sprintf(s, "%lld\n", (unsigned long long)vid);
            	writeNextResLine(s, n);
	    }
        }
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<int> {
public:
    void init() {
        m_global = 0;
        m_local = 0;
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global = * (int *)p;
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        m_global += * (int *)p;
    }
    void accumulate(const void* p) {
        m_local += * (int *)p;
    }
};

class VERTEX_CLASS_NAME(): public Vertex <MyVertex, int, bool> {
public:
    void compute(MessageIterator* pmsgs) {
 
        int delete_degree = 0;
        struct MyVertex newVertex;
        
        const int64_t n = getOutEdgeIterator().size();
        
        if (getSuperstep() == 0) {  
             newVertex.is_deleted = false;
             newVertex.current_degree = n;
             *mutableValue() = newVertex;
	printf("whole 0= %d\n", newVertex.current_degree);
        } else {
            if(getValue().is_deleted == true){ voteToHalt(); return;}
            if (getSuperstep() >= 2) {
                int global_val = * (int *)getAggrGlobal(0);
                if (global_val == 0 ) {
                    voteToHalt(); return;
                }
            }
     
            
            for ( ; ! pmsgs->done(); pmsgs->next() ) {
                delete_degree += 1;
            }
            printf("before = %d\n", getValue().current_degree);
            if(getValue().is_deleted == false){
	    	newVertex.current_degree = getValue().current_degree - delete_degree;
	    }else{
		newVertex.current_degree = getValue().current_degree;
	    }
            printf("delete = %d\n",delete_degree); 
            printf("update = %d\n",newVertex.current_degree); 	   			
        }
	
        if(newVertex.current_degree < k && getValue().is_deleted == false){
                int del = 1;
                accumulateAggr(0, &del);
                newVertex.is_deleted = true;
        	sendMessageToAllNeighbors(true);
	}
        *mutableValue() = newVertex;
    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: PageRankVertex.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    // argv[3]: <k>	
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 4) {
           printf ("Usage: %s <input path> <output path> <k>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
 	k = atoi(argv[3]);

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
