# How to Run

This guide assumes you have **Maven** and **Java 21+** installed.

## 1. Compilation

First, you need to generate the "Fat JAR" which contains the project code and all its dependencies (Babel, Log4j, etc.):

```bash
mvn clean compile package -U
```

## 2. Running the Nodes

Open 2 different terminals and run the following commands:

**Node 1:**
```bash
java -cp target/Project1.jar org.example.Main interface=eth2 port=10101
```

**Node 2:**
```bash
java -cp target/Project1.jar org.example.Main interface=eth2 port=10102 contact=<YOUR-IPV4>:<PORT-OF-NODE1>
```

## 3. Finding your Network Interface

Once you have identified your network interface, replace `eth2` in the commands above with your specific interface name.

### Linux:
```bash
ip addr
# Look for interfaces like 'eth0', 'wlp1s0', or 'enp0s3'
```

### MacOS:
```bash
ifconfig
```

### Windows:
```bash
ipconfig
```