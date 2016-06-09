     ____        _         ____                                     _
    |  _ \  __ _| |_ __ _ / ___| ___  __  __ __  __  __ _ __  _  __| |_ __ 
    | | | |/ _` | __/ _` | |    / _ \|  `´  |  `´  |/ _` |  \| |/ _  | '__/
    | |_| | (_| | || (_| | |___| (_| | |\/| | |\/| | (_| |   ' | (_| | |
    |____/ \__,_|\__\__,_|\____|\___/|_|  |_|_|  |_|\__,_|_|\__|\__,_|_|


# StreamCommandr: Integrate. Transform. Analyze. 

StreamCommandr Engine is a Java library for in-stream analytics by intergrating incoming data, transforming and analyzing it. 
Its specific feature is that it is based on a column-oriented data representation and column-oriented operations with data. 
StreamCommandr relates to conventional stream processing system like column-oriented databases relate to conventional databases. 
The column-oriented approach to in-stream analytics is especially efficient where very high performance is needed, e.g., in IoT application. 
Operations with data are performed by defining new columns in terms of already existing columns. 
StreamCommandr relies on the concept-oriented model as its theoretical foundation. 

# Major problems

## How to transform records in arbitrary format to/from our SC format/convention.

### Custom input/output stream classes

One approach is to assume that input/output stream class is intended for certain format of records. 
For example, there can be JSON input/output stream. In additional to the representation format, 
the class implements the corresponding transformation methods, that is, it knows how to transform 
its internal format to/from SC format. These transformation methods are parameterized by the 
corresponding mapping and transformation parameters. Actually, this means that we have a generic 
class for representing records in certain format and, in addition, we have a structure which 
represents certain mappings and transformations with the records of this format. Also, a stream 
could provide functionality for reading/writing its records from/to a remote source. In this case, 
it is necessary to provide connectivity parameters and probably a thread for performing this function. 
For example, an input/output stream could use a JDBC database as a source/target or a RabbitMQ message 
bus or a HTTP server as a source of JSON events via REST. 

Thus we have three pieces: 
* Certain representation format implemented by a stream class. 
* Certain mapping/transformation parameter representation and implementation in methods. 
* Connection data representation and implementation. 

### Input/output streams as normal tables

We can interpret them as normal tables in the sense that they implement at least part of table API.
For example, they could implement SC record appending and retrieving. However, they are more complicated
because they have their own process. 

Another idea is that input/output streams can be implemented as import/export columns which can be
evaluated. Such a column is able to map/transform data as well as read/write it to/from external source. 
However, the data is stored in the table. 

### External components is responsible for that

For example, we could simply embed SC engine into a web server and then manually transform input 
events into the desired format. It is probably the most efficient approach. 

A drawback of this approach is that we do not know how to automatically produce output. 
One method would be to read the results of evaluation from the same web server and use them. 

Note that we do not want to permit external connections from within column evaluators.
We want that evaluators are able to write SC data to output streams which know how 
to convert them and how to send them. 

This means that evaluators have to know about output streams and how to use them. One approach 
how it can be conceptualized is that output streams are viewed as normal (or special) tables.
Therefore, evaluators can append records to these tables (but nothing more). 

If evaluators know about output streams as tables then it would be natural to know about 
input streams as tables. In fact, we also can view input streams as normal tables. However, 
we cannot append SC records to these tables. Rather, it is only possible to append their 
specific records. Yet, it is possible to retrieve (read and remove) records. This is normally 
done by the evaluation process/driver. Theoretically, however, it is not prohibited to do it 
from evaluators. Maybe it would be good idea simply to hide input streams or somehow restrict 
their functions.  
