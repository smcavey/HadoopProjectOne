import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Random;

public class CreateData{
	
	public static void main(String [] args) {
		try (PrintWriter writer = new PrintWriter("Customers.csv")) {
			char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
			String[] genders = new String[]{"male", "female"};
			StringBuilder rowBuilder = new StringBuilder();
			StringBuilder nameBuilder = new StringBuilder();
			Random random = new Random();
			for(int i = 1; i < 50001; i++){ // write 50K entries
				rowBuilder.append(String.valueOf(i)); // add incremented ID
				rowBuilder.append(",");
				int randomNameLength = ThreadLocalRandom.current().nextInt(10, 21); // get a random name length between 10 and 20 chars
				for (int k = 0; k < randomNameLength+1; k++) {
					char c = chars[random.nextInt(chars.length)];
					nameBuilder.append(c);
				}
				rowBuilder.append(nameBuilder.toString());
				rowBuilder.append(",");
				int randomAge = ThreadLocalRandom.current().nextInt(10, 71);
				rowBuilder.append(String.valueOf(randomAge)); // get a random age between 10 and 70
				rowBuilder.append(",");
				int randomGender = ThreadLocalRandom.current().nextInt(0, 2);
				rowBuilder.append(genders[randomGender]); // get a random gender between 'male' and 'female'
				rowBuilder.append(",");
				int randomCountry = ThreadLocalRandom.current().nextInt(1, 11);
				rowBuilder.append(String.valueOf(randomCountry)); // get a random country code between 1 and 10
				rowBuilder.append(",");
				double randomSalary = ThreadLocalRandom.current().nextDouble(100, 10001);
				rowBuilder.append(String.valueOf(randomSalary)); // get a random salary between 100 and 10000
				rowBuilder.append("\n");
				writer.write(rowBuilder.toString()); // write row to csv
				rowBuilder.setLength(0); // reset row
				nameBuilder.setLength(0); // reset name
			}
			System.out.println("Done writing Customers.csv");
		} catch (FileNotFoundException e){
			System.out.println(e.getMessage());
		}
		
		try (PrintWriter writer = new PrintWriter("Transactions.csv")){
			StringBuilder rowBuilder = new StringBuilder();
			StringBuilder descBuilder = new StringBuilder();
			char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
			Random random = new Random();
			for(int i = 1; i < 5000001; i++){ // write 5M entries
				rowBuilder.append(String.valueOf(i)); // add incremented transaction id
				rowBuilder.append(",");
				int randomID = ThreadLocalRandom.current().nextInt(1, 50001);
				rowBuilder.append(String.valueOf(randomID)); // add random customer id between 1 and 50000
				rowBuilder.append(",");
				double randomTransaction = ThreadLocalRandom.current().nextDouble(10, 1001);
				rowBuilder.append(String.valueOf(randomTransaction)); // add random transaction cost between 10 and 1000
				rowBuilder.append(",");
				int numItems = ThreadLocalRandom.current().nextInt(1, 11);
				rowBuilder.append(String.valueOf(numItems)); // add a random number of items purchased between 1 and 10
				rowBuilder.append(",");
				int randomDescLength = ThreadLocalRandom.current().nextInt(20, 51);
				for (int k = 0; k < randomDescLength+1; k++) { // create a random description
					char c = chars[random.nextInt(chars.length)];
					descBuilder.append(c);
				}
				rowBuilder.append(descBuilder.toString());
				rowBuilder.append("\n");
				writer.write(rowBuilder.toString());
				rowBuilder.setLength(0); // reset row
				descBuilder.setLength(0); // reset description
			}
			System.out.println("Done writing Transactions.csv");
		} catch (FileNotFoundException e){
			System.out.println(e.getMessage());
		}
	}
}