// See https://aka.ms/new-console-template for more information

using Lux;
using Nixie;
using Lux.Discovery;

Console.WriteLine("Hello, World!");

//var aas = new ActorSystem();
//var p = new RaftManager(aas);

await Task.WhenAll(
    Multicast.StartBroadcasting(),
    Multicast.StartListening()
);    