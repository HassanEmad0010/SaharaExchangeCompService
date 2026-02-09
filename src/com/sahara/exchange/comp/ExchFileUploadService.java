package com.sahara.exchange.comp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import com.temenos.api.TStructure;
import com.temenos.t24.api.complex.eb.servicehook.ServiceData;
import com.temenos.t24.api.complex.eb.servicehook.TransactionData;
import com.temenos.t24.api.hook.system.ServiceLifecycle;
import com.temenos.t24.api.records.account.AccountRecord;
import com.temenos.t24.api.records.ebfileuploadtype.EbFileUploadTypeRecord;
import com.temenos.t24.api.records.fundstransfer.FundsTransferRecord;
import com.temenos.t24.api.system.DataAccess;
import com.temenos.t24.api.tables.stmbscatmfileuploadparam.StMbscAtmFileuploadParamRecord;

/**
 * TODO: Document me!
 *
 * @author Hassan
 * Service to upload the exchange company file, it will debit the file account and credit the param one *
 */
public class ExchFileUploadService extends ServiceLifecycle {
    
    String mainPath = "";

    String delimiter = "\\";
  
    String paramUploadDir="" ;

    String paramArchiveDir="";





  
  
  @Override
  public List<String> getIds(ServiceData serviceData, List<String> controlList) {
      DataAccess da = new DataAccess(this);



      EbFileUploadTypeRecord eBRecord = new EbFileUploadTypeRecord(
              da.getRecord("EB.FILE.UPLOAD.TYPE", "MBSC.ATM.FILE"));
      
      StMbscAtmFileuploadParamRecord stMbscParam = new StMbscAtmFileuploadParamRecord(
              da.getRecord("ST.MBSC.ATM.FILEUPLOAD.PARAM", "SYSTEM"));

       mainPath = stMbscParam.getFuture2().getValue();

       paramUploadDir = eBRecord.getUploadDir().getValue();
      
       paramArchiveDir = stMbscParam.getFuture1().getValue();
       





      String tempFileDir ="TEMP";
      String filesPath = mainPath.concat(delimiter).concat(paramUploadDir).concat(delimiter).concat(tempFileDir);
      String archivePath =mainPath.concat(delimiter).concat(paramArchiveDir);

      File uploadDirectory = new File(filesPath);
      File archiveDirectory = new File(archivePath);
      


      // Create archive directory if it does not exist
      if (!archiveDirectory.exists()) {
          try {
              archiveDirectory.mkdirs();
              System.out.println("Created directory: " + archivePath);
          } catch (Exception e) {
              System.out.println("Can't create the archive directory: " + e.getMessage());
             // throw new Error("Can't create the archive directory: " + e.getMessage());
          }
      }
      



      List<String> csvFiles = new ArrayList<>();

      // Check if directory exists and list CSV files
      if (uploadDirectory.exists() && uploadDirectory.isDirectory()) {
          File[] files = uploadDirectory.listFiles();
          for (File file : files) {
              if (file.isFile() && file.getName().endsWith(".csv")) {
                  csvFiles.add(file.getAbsolutePath());
                  System.out.println("CSV file added: " + file.getAbsolutePath());
              }
          }
      }

      // Ensure at least one CSV file exists
      if (csvFiles.isEmpty()) {
          System.out.println("No CSV files found in directory. csv file:"+ csvFiles);
          System.out.println("filesPath :"+filesPath );
          System.out.println("archivePath :"+archivePath);
          System.out.println("uploadDirectory.getAbsoluteFile() :"+uploadDirectory.getAbsoluteFile());
          System.out.println("archiveDirectory.getAbsolutePath :"+archiveDirectory.getAbsolutePath());
          System.out.println("archivePath"+archivePath);

      }

      System.out.println("======List of csv files selected: "+ csvFiles+" =========");
      
      return csvFiles;
  }


  
  
  
  //======================================================================
  
  
  @Override
  public void postUpdateRequest(String csvFilePath, ServiceData serviceData, String controlItem,
          List<TransactionData> transactionData, List<TStructure> records) {

      String paramCreditAccount ="";

      DataAccess da = new DataAccess(this);
      StMbscAtmFileuploadParamRecord stMbscParam = new StMbscAtmFileuploadParamRecord(
              da.getRecord("ST.MBSC.ATM.FILEUPLOAD.PARAM", "SYSTEM"));
      paramCreditAccount = stMbscParam.getFuture3().getValue();

      
      String archiveDirPath = paramArchiveDir;


      String archiveFileAbsolutePath =mainPath.concat(delimiter).concat(archiveDirPath);

      
      File csvFile = new File(csvFilePath);
      File archiveFile = new File(archiveFileAbsolutePath, csvFile.getName());

      String csvFileName = csvFile.getName();

      // Read and process the CSV file
      try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
          String line;
          boolean isHeader = true;

          while ((line = br.readLine()) != null) {
              // Skip the header row
              if (isHeader) {
                  isHeader = false;
                  continue;
              }

              // Split the line into columns
              String[] columns = line.split(",", -1); // Use -1 to include empty strings
              if (columns.length < 3) {
                  System.out.println("Skipping row due to insufficient columns: " + line);
                  continue;
              }

              // Extract data from the columns
              String fileDebitAccount = columns[2].trim();
              String netAmount = columns[5].trim();
              String processingDate = columns[6].trim();

              // Validate required fields
              if (fileDebitAccount.isEmpty() || netAmount.isEmpty() || processingDate.isEmpty()) {
                  System.out.println("Skipping row due to missing data: " + line);
                  continue;
              }

              // Log the extracted data
              System.out.println("Processing row: Account No: " + fileDebitAccount +
                      ", Net Amount: " + netAmount + ", Processing Date: " + processingDate);

              // Retrieve the debit account and currency
              String debitCurrency ="";
              if (! (paramCreditAccount.isEmpty()) )
                  
              {AccountRecord acRecord = new AccountRecord(da.getRecord("ACCOUNT", fileDebitAccount));
               debitCurrency = acRecord.getCurrency().getValue();
              }
              else 
              {System.out.println("debit account not exists on the param template");}
              
              // Create and populate a FundsTransferRecord
              FundsTransferRecord ft = new FundsTransferRecord(this);
              ft.setTransactionType("ACEC");
              ft.setDebitAcctNo(fileDebitAccount);
              ft.setCreditAcctNo(paramCreditAccount);
              ft.setDebitAmount(netAmount);
              ft.setDebitCurrency(debitCurrency);

              try {
                  ft.getLocalRefField("MBSC.ATMUP.FILENAME").set(csvFileName);
                  ft.getLocalRefField("MBSC.ATMUP.DATE").set(processingDate);
              } catch (IllegalArgumentException e) {
                  System.out.println("Error setting local fields: " + e.getMessage());
                 // throw new Error("Error setting local fields: " + e.getMessage());
              }

              // Create transaction data
              TransactionData tranData = new TransactionData();
              tranData.setSourceId("MBSC.ATM.UPLOAD");
              tranData.setFunction("INPUT");
              tranData.setVersionId("FUNDS.TRANSFER,MBSC.ATM.FILEUPLOAD");
              tranData.setNumberOfAuthoriser("1");

              // Add to records and transaction data lists
              records.add(ft.toStructure());
              transactionData.add(tranData);
              System.out.println("Added Transaction ID: " + tranData.getTransactionId());
          }

      } catch (IOException e) {
          e.printStackTrace();
          System.out.println("Error processing CSV file: " + csvFilePath);
        //  throw new Error("Error processing CSV file: " + csvFilePath);
      }

      // Move the processed file to the archive directory
      Path sourcePath = csvFile.toPath();
      Path targetPath = archiveFile.toPath();

      try {
          Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
          System.out.println("Moved file to archive: " + targetPath);
      } catch (IOException e) {
          System.out.println("Error when moving to archive: " + e.getMessage());
      }

  }


}
