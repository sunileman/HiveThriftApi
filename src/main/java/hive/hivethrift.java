package hive;
import java.util.Iterator;
import java.util.List;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
/*
 *  to make it work add the hive-service and hadoop-client dependencies
 *  in pom.xml
 */
public class hivethrift {
    public static void main(String[] args) throws TException {
        TSocket transport = new TSocket("127.0.0.1", 10000);

        transport.setTimeout(999999999);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        TCLIService.Client client = new TCLIService.Client(protocol);

        System.out.println("here1");
        transport.open();
        System.out.println("here2");
        TOpenSessionReq openReq = new TOpenSessionReq();
        System.out.println("here3");
        TOpenSessionResp openResp = client.OpenSession(openReq);
        System.out.println("here4");
        TSessionHandle sessHandle = openResp.getSessionHandle();
        System.out.println("here5");

        TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "show tables");
        TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
        TOperationHandle stmtHandle = execResp.getOperationHandle();

        TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle, TFetchOrientation.FETCH_FIRST, 100);
        TFetchResultsResp resultsResp = client.FetchResults(fetchReq);
        List<TColumn> res=resultsResp.getResults().getColumns();
        for(TColumn tCol: res){
            Iterator<String> it = tCol.getStringVal().getValuesIterator();
            while (it.hasNext()){
                System.out.println(it.next());
            }
        }

        TCloseOperationReq closeReq = new TCloseOperationReq();
        closeReq.setOperationHandle(stmtHandle);
        client.CloseOperation(closeReq);
        TCloseSessionReq closeConnectionReq = new TCloseSessionReq(sessHandle);
        client.CloseSession(closeConnectionReq);

        transport.close();
    }

}