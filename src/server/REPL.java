package server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * State: running state
 */
class State{
    public static final int EXIT_ERROR = -1;
    public static final int EXIT_SUCCESS = 0;
}

/**
 * InputBuffer: get from the std input
 */
class InputBuffer{
    int bufSize;
    int inputSize;
    private BufferedReader inputBuffer;
    private String readFromConsole;
    String[] words;

    InputBuffer() throws IOException {
        this.inputSize = 0;
        inputBuffer = new BufferedReader(new InputStreamReader(System.in));
    }
    /**
      read from the console
     */
    void read() throws IOException {
        readFromConsole = inputBuffer.readLine();//读取当前行
        words = readFromConsole.split(" ");//用空格分开不同的单词
        if(words.length <= 0){//单词个数小于0，出错
            System.out.println("Error Reading Input\n");
            close(-1);//错误退出
        }
        inputSize = words.length;//把读到的单词数存到inputSize里面
    }

    String getReadFromConsole(){
        return readFromConsole;
    }

    void close(int state){
        System.exit(state);
    }
}

/**
 * print the commander decorator
 */
class Print{
    public void printPrompt(){
        System.out.print("db > ");
    }
}

/**
 * Start the DBServer
 */
public class REPL {
    public static void main(String[] args) throws IOException {
        int argc = args.length;
        if(argc >= 0) {
            InputBuffer inputBuffer = new InputBuffer();
            Print infoPrint = new Print();
            State state = new State();

            while (true) {
                infoPrint.printPrompt();
                inputBuffer.read();
                if (inputBuffer.words[0].equals(".exit")) { //退出执行
                    inputBuffer.close(State.EXIT_SUCCESS);//正常退出
                } else {
                    System.out.println("UnRecognized Command '" + inputBuffer.getReadFromConsole() + "'");
                }
            }
        }
    }
}
