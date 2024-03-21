import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.*;

class DBConnect{
    Connection con;
    Statement st;
    public DBConnect(){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/poll_app?characterEncoding=utf8", "root", "");
            st = con.createStatement();

        }
        catch (Exception e) {
          System.out.println("Error "+e);
       }
    }
}

class ClientHandler extends Thread{
    private Socket clientSocket;
    private DBConnect dbcon;
    public ClientHandler(Socket clientSocket, DBConnect dbcon) {
        this.clientSocket = clientSocket;
        this.dbcon = dbcon;
    }
    public void run() {
        try{
            DataInputStream sin = new DataInputStream(clientSocket.getInputStream());
            DataOutputStream sout = new DataOutputStream(clientSocket.getOutputStream());
            String str;
            while(true){
                str = sin.readUTF();
                if(str.equals("insert")){
                    str = sin.readUTF();
                    System.out.println(str);
                    dbcon.st.executeUpdate(str);
                }
                else if(str.equals("poll_insert")){
                    str = sin.readUTF();
                    System.out.println(str);
                    String arr[] = {"poll_id"};
                    int ar = dbcon.st.executeUpdate(str, dbcon.st.RETURN_GENERATED_KEYS);
                    System.out.println("hi");
                    ResultSet rs = dbcon.st.getGeneratedKeys();
                    rs.next();
                    int id = rs.getInt(1);
                    sout.writeInt(id);
                }
                if(str.equals("select")){
                    str = sin.readUTF();
                    System.out.println(str);
                    ResultSet rs = dbcon.st.executeQuery(str);
                    if(rs.next()){
                        sout.writeBoolean(true);
                        sout.writeUTF(rs.getString(1));
                    }
                    else
                        sout.writeBoolean(false);
                }
                if(str.equals("select-poll")){
                    str = sin.readUTF();
                    System.out.println(str);
                    ResultSet rs = dbcon.st.executeQuery(str);
                    if(rs.next()){
                        sout.writeBoolean(true);
                        sout.writeUTF(rs.getString("poll_question"));
                        rs = dbcon.st.executeQuery("select * from tbl_options where poll_id = "+rs.getString("poll_id"));
                        int rowCount = 0;
                        while(rs.next()) {
                            rowCount++;
                        }
                        rs.first();
                        System.out.println(rowCount);
                        sout.writeInt(rowCount);
                        do{
                            sout.writeUTF(rs.getString("option_value"));
                        }while(rs.next());
                        if(sin.readInt() == 1){
                            rs.first();
                            do{
                                sout.writeInt(rs.getInt("votes"));
                            }while(rs.next());
                        }
                        
                    }
                    else
                        sout.writeBoolean(false);
                }
                if(str.equals("vote")){
                    str = sin.readUTF();
                    // int id = sin.readInt();
                    System.out.println(str);
                    dbcon.st.executeUpdate(str);
                    // ResultSet rs = dbcon.st.executeQuery("select * from tbl_options where poll_id = "+id);
                    
                }
        }

        } catch (IOException  | SQLException e) {
            System.out.println("Error handling client: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) clientSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
}

public class DBServer{
    
    public static void main(String[] args) {
        String str;
        ServerSocket ss;
        try {
            ss = new ServerSocket(1234);
            DBConnect dbcon = new DBConnect();
            System.out.println("Conncected");
            while (true) {
                Socket clientSocket = ss.accept();
                System.out.println("Client connected: " + clientSocket);
                ClientHandler clientHandler = new ClientHandler(clientSocket, dbcon);
                clientHandler.start();
            }
        }
         catch (Exception e) {
           System.out.println("Error "+e);
        }
    }
}