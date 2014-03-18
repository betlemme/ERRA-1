/*
Pacchetto dati:
                1 byte con valore compreso tra 0 e 250, che indica il numero di indirizzi nella lista
                4 byte con l'indirizzo della sorgente
                1 byte con numero di messaggi mandati dalla sorgente nel momento dell'invio
                1 byte con numero di pacchetto (cioè numero di parte del file)
                8 byte con la lunghezza del file
                lista di indirizzi (4 byte per ogni indirizzo)
                byte del file

Pacchetto inviato al bootstrap quando un nodo si connette alla rete:
                1 byte con valore 251  (l'indirizzo del nodo che si connette è ricavato dal socket)

Pacchetto inviato al bootstrap quando un nodo si disconnette dalla rete:
                1 byte con valore 252  (l'indirizzo del nodo che si disconnette è ricavato dal socket)

Pacchetto per richiedere la lista dei nodi al bootstrap:
                1 byte con valore 253
                indirizzo destinazione (4 byte) (bisogna far sapere al bootstrap l'indirizzo di destinazione perché è quello che va 
                                                                                 messo alla fine della lista)

Pacchetto di risposta dal bootstrap con la lista di indirizzi in ordine NON casuale (lo shuffle viene fatto nel nodo sorgente):
                1 byte con valore 254
                lista di indirizzi

Pacchetto di risposta dal bootstrap che indica che il nodo destinazione richiesto non esiste nella rete:
                1 byte con valore 255

 */


// Il programma viene fatto partire con "java ERRANode <indirizzo bootstrap>"

import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

public class ERRANode {
	private static final int PKT_LENGTH = 40000;
	private static final int CONNECTION_CODE = 251;
	private static final int DISCONNECTION_CODE = 252;
	private static final int ADDRESSES_LIST_REQUEST_CODE = 253;
	private static final int BOOTSTRAP_RESPONSE_SUCCESS_CODE = 254;
	private static final int BOOTSTRAP_RESPONSE_ERROR_CODE = 255;

	private static String bootstrapAddress;
	private static InetAddress myIP = null;
	private static int numberMSGSent = 0;
	private static HashMap<String, byte[]> MSGList = new HashMap<String, byte[]>();


	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Specify the bootstrap address");
		}
		else {
			bootstrapAddress = args[0];
			joinNetwork();
			PacketListener pl = new PacketListener();
			pl.setDaemon(true);
			pl.start();
			waitForMessage();
		}
	}

	private static void waitForMessage() {
		String name = "";
		Scanner in = new Scanner(System.in);

		while (true) {
			System.out.print("\nName of file to send (/logout to disconnect): ");
			name = in.nextLine();
			if (name.equals("/logout"))
				break;
			sendFile(name);
		}

		in.close();
		disconnectFromNetwork(myIP);
	}

	private static void joinNetwork() {
		Socket bootstrapSocket = null;
		try {
			bootstrapSocket = new Socket(bootstrapAddress, 10000);
			DataOutputStream output = new DataOutputStream(bootstrapSocket.getOutputStream());
			output.writeByte(CONNECTION_CODE);
			myIP = bootstrapSocket.getLocalAddress();    // l'ip di questo nodo viene ricavato direttamente dal socket
		} catch (IOException e) {
			System.err.println("Problem connecting to the ERRA network");
		} finally {    // i socket vanno chiusi sia che ci sia un errore sia che non ci sia, quindi la chiusura va nel finally
			try {
				if (bootstrapSocket != null)
					bootstrapSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private static void disconnectFromNetwork(InetAddress addr) {
		Socket bootstrapSocket = null;
		try {
			bootstrapSocket = new Socket(bootstrapAddress, 10000);
			DataOutputStream output = new DataOutputStream(bootstrapSocket.getOutputStream());
			output.writeByte(DISCONNECTION_CODE);
			output.write(addr.getAddress(), 0, 4);
		} catch (IOException e) {
			System.err.println("Problem disconnecting from the ERRA network");
		} finally {
			try {
				if (bootstrapSocket != null)
					bootstrapSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static void sendFile(String fileName) {
				
		numberMSGSent++;

		Socket outputSocket = null;
		try {
			InetAddress destination = InetAddress.getByName(fileName);
			ArrayList<InetAddress> addressesList = requestAddressesList(destination);
			if (addressesList == null) {
				System.out.println("The specified destination is not connected to the ERRA network");
				return;
			}

			ArrayList<File> pktMSG = new ArrayList<File>();
			File f = new File(fileName);
			long MSGLength = f.length();
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
			FileOutputStream out;
			byte partCounter = 0;
			byte[] buffer = new byte[PKT_LENGTH];
			int tmp = 0;
			while ((tmp = bis.read(buffer)) > 0) {   // split del file
				File newFile = new File(fileName + "." + String.format("%03d", partCounter & 0xFF));
				newFile.createNewFile();
				out = new FileOutputStream(newFile);
				out.write(buffer,0,tmp);
				pktMSG.add(newFile);
				partCounter++;
				out.close();
			}
			bis.close();

			for (File pkt : pktMSG) {

				String pktName = pkt.getName(); 
				short numberPKT = Short.parseShort(pktName.split("\\.")[4]);  // il file ha come nome "a.b.c.d.e", "e" è quello che ci serve

				Collections.shuffle(addressesList, new Random(System.nanoTime()));
				
				outputSocket = null;
				int i = 0;
				while (outputSocket == null) {
					InetAddress nextNode = null;
					try {
						if (i >= addressesList.size()) {
							System.out.println("Lista nodi finita");
							return;
						}
						nextNode = addressesList.get(i);
						outputSocket = new Socket(nextNode, 10000);
					} catch (ConnectException e) {
						disconnectFromNetwork(nextNode);
					}
					i++;
				}
				
				try {
					DataOutputStream output = new DataOutputStream(new BufferedOutputStream(outputSocket.getOutputStream(), PKT_LENGTH + 15 + 4*addressesList.size()));
					output.writeByte(addressesList.size() - i + 1);
					output.write(myIP.getAddress(), 0, 4); 
					output.writeByte(numberMSGSent);
					output.writeByte(numberPKT);
					output.writeLong(MSGLength);
	
					for (; i < addressesList.size(); i++) 
						output.write(addressesList.get(i).getAddress(), 0, 4);   // ogni indirizzo viene mandato come gruppo di 4 byte
					output.write(destination.getAddress(), 0, 4);   // mette in fondo alla lista l'indirizzo della destinazione
	
					byte[] buffer2 = new byte[PKT_LENGTH];
					int len;
					FileInputStream fis = new FileInputStream(pkt);
					while ((len = fis.read(buffer2)) > 0)
						output.write(buffer2, 0, len);
					
					output.flush();
					fis.close();
				} catch (IOException e) {
					System.err.println("Problem sending file");
				} finally {
					try {
						if (outputSocket != null)
							outputSocket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		} catch (IOException e) {
			System.err.println("Problem sending file");
		} finally {
			try {
				if (outputSocket != null)
					outputSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static ArrayList<InetAddress> requestAddressesList(InetAddress destination) {
		Socket bootstrapSocket = null;
		ArrayList<InetAddress> addressesList = new ArrayList<InetAddress>();
		try {
			bootstrapSocket = new Socket(bootstrapAddress, 10000);
			DataOutputStream bootstrapOutput = new DataOutputStream(new BufferedOutputStream(bootstrapSocket.getOutputStream(), 5));
			bootstrapOutput.writeByte(ADDRESSES_LIST_REQUEST_CODE);
			bootstrapOutput.write(destination.getAddress(), 0, 4);
			bootstrapOutput.flush();

			DataInputStream input = new DataInputStream(new BufferedInputStream(bootstrapSocket.getInputStream(), 1 + 256));
			byte firstByte = input.readByte();
			if ((firstByte & 0xFF) == BOOTSTRAP_RESPONSE_ERROR_CODE)
				return null;  // la destinazione non esiste

			byte[] buffer = new byte[4];
			int len;
			while ((len = input.read(buffer)) > 0) {
				addressesList.add(InetAddress.getByAddress(buffer));
			}
		} catch (IOException e) {
			System.err.println("Problem requesting addresses list to bootstrap");
		} finally {
			try {
				if (bootstrapSocket != null)
					bootstrapSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return addressesList;
	}


	private static class PacketListener extends Thread {

		public void run() {
			ServerSocket server = null;
			try {
				server = new ServerSocket(10000);
			} catch (IOException e) {
				System.err.println("Problem creating this node's server");
			} 

			while (true) {
				System.out.println("Waiting for packets");
				Socket inputSocket = null;
				try {
					inputSocket = server.accept();
					DataInputStream input = new DataInputStream(new BufferedInputStream(inputSocket.getInputStream(), PKT_LENGTH + 15 + 4*256));
					byte firstByte = input.readByte();
					if (firstByte == 0) {     // il pacchetto è arrivato a destinazione
						saveFileFragment(input);
					} else {
						forwardPacket(input, firstByte);
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						if (inputSocket != null)
							inputSocket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		private void forwardPacket(DataInputStream input, byte remainingNodesNum) {
			Socket outputSocket = null;
			try {
				// leggo tutte le varie parti dell'header separatamente solo perché mi serve l'id del messaggio e il numero di pacchetto
				// da stampare sul terminale
				byte[] sourceAddress = new byte[4];
				input.read(sourceAddress);
				byte numberMSGSent = input.readByte();
				int numberOfReceivedPkt = input.readByte() & 0xFF;
				long MSGLength = input.readLong();
				
				String idMSG = InetAddress.getByAddress(sourceAddress).getHostAddress() + "." + numberMSGSent;
				
				byte[] nextHopAddress = new byte[4];
				
				while (outputSocket == null) {
					try {
						input.read(nextHopAddress);
						outputSocket = new Socket(InetAddress.getByAddress(nextHopAddress), 10000);
					} catch (ConnectException e) {
						disconnectFromNetwork(InetAddress.getByAddress(nextHopAddress));
						remainingNodesNum--;
					}
				}
				
				DataOutputStream output = new DataOutputStream(new BufferedOutputStream(outputSocket.getOutputStream(), PKT_LENGTH + 15 + 4*(remainingNodesNum - 1)));
				output.writeByte(remainingNodesNum - 1);
				output.write(sourceAddress, 0, 4); 
				output.writeByte(numberMSGSent);
				output.writeByte(numberOfReceivedPkt);
				output.writeLong(MSGLength);

				byte[] buffer = null;
				if (numberOfReceivedPkt == MSGLength/PKT_LENGTH) {   // se il pacchetto è l'ultimo
					buffer = new byte[(int)(MSGLength%PKT_LENGTH) + 4*(remainingNodesNum - 1)];
				} else {
					buffer = new byte[PKT_LENGTH + 4*(remainingNodesNum - 1)];
				}
				input.readFully(buffer);
				output.write(buffer, 0, buffer.length);
				output.flush();
				System.out.println("Packet " + numberOfReceivedPkt + " of message " + idMSG + " forwarded (remaining nodes: " + (remainingNodesNum & 0xFF) + ")");
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (outputSocket != null)
						outputSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		private void saveFileFragment(DataInputStream input) {
			try {
				byte[] sourceAddress = new byte[4];
				input.read(sourceAddress);
				byte numberMSGSent = input.readByte();
				int numberOfReceivedPkt = input.readByte() & 0xFF;
				long MSGLength = input.readLong();
				String idMSG = InetAddress.getByAddress(sourceAddress).getHostAddress() + "." + numberMSGSent;
				System.out.println("Packet " + numberOfReceivedPkt + " of message " + idMSG + " reached the destination");

				if (!MSGList.containsKey(idMSG)) {
					byte[] tmp = new byte[(int) (1 + MSGLength)];
					tmp[0] = (byte) Math.ceil((((double)(MSGLength))/PKT_LENGTH));
					MSGList.put(idMSG, tmp);
				}
				int offset = 1 + PKT_LENGTH * numberOfReceivedPkt;
				byte[] buffer = null;
				if (numberOfReceivedPkt == MSGLength/PKT_LENGTH) {   // se il pacchetto è l'ultimo
					buffer = new byte[(int)(MSGLength%PKT_LENGTH)];
				} else {
					buffer = new byte[PKT_LENGTH];
				}
				input.readFully(buffer);
				System.arraycopy(buffer, 0, MSGList.get(idMSG), offset, buffer.length);

				byte[] tmp1 = MSGList.get(idMSG);
				if (--tmp1[0] == 0) {    // se abbiamo tutti i pacchetti
					saveFile(tmp1, idMSG);
					MSGList.remove(idMSG);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void saveFile(byte[] tmp, String idMSG) {
			FileOutputStream fos = null;
			try {
				fos = new FileOutputStream(idMSG);
				fos.write(tmp, 1, tmp.length - 1);

				System.out.println("File saved as " + idMSG);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (fos != null)
						fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}			
		}

	}

}