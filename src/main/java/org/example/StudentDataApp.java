package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.example.data.RequestData;
import org.example.data.ResponseData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Insert data using lambda function and aws rds mysql!
 * mvn clean package shade:shade
 * {
 *     "studentRoll" : "1",
 *     "studentName" : "Gopi Bhowmick",
 *     "studentStream" : "BCA",
 *     "studentTotalMarks" : "9.8",
 *     "studentAddress" : "Kolkata"
 * }
 */
public class StudentDataApp implements RequestHandler<RequestData, ResponseData>
{

    @Override
    public ResponseData handleRequest(RequestData requestData, Context context) {
        ResponseData responseData = new ResponseData();
        if (requestData.getStudentRoll()!=null && requestData.getStudentName()!=null && requestData.getStudentStream()!=null && requestData.getStudentAddress()!=null && requestData.getStudentTotalMarks()!=null) {
            try {
                insertData(requestData, responseData);
            } catch (SQLException sqlException) {
                responseData.setMessageId("999");
                responseData.setMessage("Unable to register " + sqlException);
            }
        } else {
            responseData.setMessageId("999");
            responseData.setMessage("Invalid request");
        }
        return responseData;
    }

    private void insertData(RequestData requestData, ResponseData responseData) throws SQLException{
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        String query = getQuery(requestData);
        int responseCode = statement.executeUpdate(query);
        if (1 == responseCode){
            responseData.setMessageId(String.valueOf(responseCode));
            responseData.setMessage("Successful updated data");
        }
    }

    private String getQuery(RequestData requestData){
        String query = "INSERT INTO student(student_roll, student_name, student_stream, student_total_marks, student_address, time) VALUES (";
        if (requestData != null){
            query = query.concat("'"+requestData.getStudentRoll()+"','"+requestData.getStudentName()+"','"+requestData.getStudentStream()+"','"+requestData.getStudentTotalMarks()+"','"+requestData.getStudentAddress()+"', CURRENT_TIMESTAMP())");
        }
        return query;
    }

    private Connection getConnection() throws SQLException{
        String url = "jdbc:mysql://localhost:3306/gopidatbase";
        String username = "root";
        String password = "";
        Connection con = DriverManager.getConnection(url, username, password);
        return con;
    }
}
